// Common utility functions used across Flowlord pages
(function() {
    'use strict';

    let activeCell = null;
    let activeActionBar = null;
    let documentListenersBound = false;

    // Escape HTML for safe display in innerHTML
    function escapeHtml(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    // Escape text for use in HTML attributes
    function escapeAttr(text) {
        if (text === null || text === undefined) return '';
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    // Escape text for use in inline JavaScript attributes
    function escapeJsString(text) {
        if (text === null || text === undefined) return '';
        return String(text).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
    }

    function copyViaExecCommand(text) {
        const textArea = document.createElement('textarea');
        textArea.value = text;
        textArea.setAttribute('readonly', '');
        textArea.style.position = 'fixed';
        textArea.style.left = '-9999px';
        document.body.appendChild(textArea);
        textArea.select();
        textArea.setSelectionRange(0, text.length);

        let ok = false;
        try {
            ok = document.execCommand('copy');
        } catch (err) {
            console.error('execCommand copy failed: ', err);
        }
        document.body.removeChild(textArea);
        return ok;
    }

    function selectCellText(cell) {
        if (!cell) return;
        const range = document.createRange();
        range.selectNodeContents(cell);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
    }

    // Copy to clipboard functionality with enhanced feedback
    function copyToClipboard(text, sourceEvent, cell) {
        const targetElement = cell || (sourceEvent && sourceEvent.target) || document.activeElement;

        function succeed() {
            showCopyFeedback(targetElement, 'Copied!');
        }

        function tryExecCommandFallback() {
            if (copyViaExecCommand(text)) {
                succeed();
                return true;
            }
            if (cell) {
                selectCellText(cell);
                showCopyFeedback(targetElement, 'Press Ctrl/Cmd+C', true);
            }
            return false;
        }

        // Clipboard API requires a secure context (HTTPS or localhost).
        if (!window.isSecureContext || !navigator.clipboard || !navigator.clipboard.writeText) {
            tryExecCommandFallback();
            return;
        }

        navigator.clipboard.writeText(text).then(succeed).catch(function(err) {
            console.error('Could not copy text: ', err);
            tryExecCommandFallback();
        });
    }

    // Show copy feedback with animation
    function showCopyFeedback(element, message, isError) {
        isError = isError || false;

        const existingFeedback = document.querySelector('.copy-feedback');
        if (existingFeedback) {
            existingFeedback.remove();
        }

        const feedback = document.createElement('div');
        feedback.className = 'copy-feedback';
        feedback.textContent = message;
        feedback.style.backgroundColor = isError ? '#dc3545' : '#28a745';

        const rect = element.getBoundingClientRect();
        feedback.style.position = 'fixed';
        feedback.style.left = (rect.left + rect.width / 2) + 'px';
        feedback.style.top = (rect.top - 10) + 'px';
        feedback.style.transform = 'translateX(-50%)';

        document.body.appendChild(feedback);

        setTimeout(function() {
            if (feedback.parentNode) {
                feedback.remove();
            }
        }, 2000);
    }

    function copyableText(cell) {
        if (!cell) return '';
        return (cell.getAttribute('data-full-text') || cell.textContent || '').trim();
    }

    function filterValue(cell) {
        if (!cell) return '';
        return (cell.getAttribute('data-filter-value') || copyableText(cell)).trim();
    }

    function hasNestedContent(cell) {
        return cell.querySelector('a, button, input, select') !== null;
    }

    function expandCell(cell) {
        if (cell.classList.contains('expandable')) {
            cell.classList.add('expanded');
        }
        if (!cell.classList.contains('truncated') || !cell.hasAttribute('data-full-text')) {
            return;
        }

        cell.classList.remove('truncated');
        cell.classList.add('expanded');

        // Preserve links and other nested markup; CSS handles truncation.
        if (hasNestedContent(cell)) {
            return;
        }

        if (!cell.hasAttribute('data-original-html')) {
            cell.setAttribute('data-original-html', cell.innerHTML);
        }
        cell.textContent = cell.getAttribute('data-full-text');
    }

    function collapseCell(cell) {
        cell.classList.remove('cell-active');

        if (cell.hasAttribute('data-original-html')) {
            cell.innerHTML = cell.getAttribute('data-original-html');
            cell.removeAttribute('data-original-html');
            cell.classList.remove('expanded');
            cell.classList.add('truncated');
            return;
        }

        if (cell.hasAttribute('data-truncated-text')) {
            cell.classList.remove('expanded');
            cell.classList.add('truncated');
            cell.textContent = cell.getAttribute('data-truncated-text');
        } else if (cell.classList.contains('expandable') || cell.classList.contains('expanded')) {
            cell.classList.remove('expanded');
            if (cell.hasAttribute('data-full-text')) {
                cell.classList.add('truncated');
            }
        }
    }

    function removeActionBar() {
        if (activeActionBar) {
            activeActionBar.remove();
            activeActionBar = null;
        }
    }

    function positionActionBar(bar, cell) {
        const rect = cell.getBoundingClientRect();
        bar.style.position = 'fixed';
        bar.style.top = Math.max(8, rect.top + 4) + 'px';
        bar.style.left = Math.min(window.innerWidth - bar.offsetWidth - 8, rect.right - bar.offsetWidth - 4) + 'px';
    }

    const COPY_ICON_SVG = '<svg class="cell-action-icon" viewBox="0 0 16 16" width="14" height="14" aria-hidden="true">' +
        '<path fill="currentColor" fill-rule="evenodd" d="M4 2a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V2zm2-1a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V2a1 1 0 0 0-1-1H6zM2 5a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-1h1v1a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h1v1H2z"/>' +
        '</svg>';

    function showActionBar(cell, options) {
        removeActionBar();

        options = options || {};
        const onFilter = options.onFilter || null;

        const bar = document.createElement('div');
        bar.className = 'cell-actions';
        bar.setAttribute('role', 'toolbar');

        const copyBtn = document.createElement('button');
        copyBtn.type = 'button';
        copyBtn.className = 'cell-action-btn cell-action-copy';
        copyBtn.innerHTML = COPY_ICON_SVG;
        copyBtn.setAttribute('aria-label', 'Copy cell value');
        copyBtn.setAttribute('title', 'Copy');
        copyBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            copyCell(cell, e);
        });
        bar.appendChild(copyBtn);

        const filterKey = cell.getAttribute('data-filter-key');
        if (filterKey && onFilter) {
            const filterBtn = document.createElement('button');
            filterBtn.type = 'button';
            filterBtn.className = 'cell-action-btn cell-action-filter';
            filterBtn.innerHTML = '<span class="cell-action-icon icon-filter" aria-hidden="true"></span>';
            filterBtn.setAttribute('aria-label', 'Filter to this value');
            filterBtn.setAttribute('title', 'Filter to');
            filterBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                onFilter(filterKey, filterValue(cell), cell);
                deactivateCell();
            });
            bar.appendChild(filterBtn);
        }

        document.body.appendChild(bar);
        positionActionBar(bar, cell);
        activeActionBar = bar;
    }

    function deactivateCell() {
        removeActionBar();
        if (activeCell) {
            collapseCell(activeCell);
            activeCell = null;
        }
    }

    function activateCell(cell, options) {
        if (activeCell === cell) {
            return;
        }
        deactivateCell();
        activeCell = cell;
        cell.classList.add('cell-active');
        expandCell(cell);
        showActionBar(cell, options);
    }

    function copyCell(cell, sourceEvent) {
        copyToClipboard(copyableText(cell), sourceEvent, cell);
    }

    function bindDocumentListeners() {
        if (documentListenersBound) return;
        documentListenersBound = true;

        document.addEventListener('click', function(e) {
            if (!activeCell) return;
            if (e.target.closest('.cell-actions')) return;
            if (e.target.closest('.copyable') === activeCell) return;

            const table = activeCell.closest('table');
            if (table && table.contains(e.target) && e.target.closest('.copyable')) {
                return;
            }

            deactivateCell();
        });

        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && activeCell) {
                deactivateCell();
            }
        });

        document.addEventListener('scroll', function() {
            if (activeCell) {
                deactivateCell();
            }
        }, true);
    }

    // Click-to-expand with inline Copy / Filter action bar for .copyable cells.
    function enableCellActions(root, options) {
        const el = typeof root === 'string' ? document.querySelector(root) : root;
        if (!el) return;

        options = options || {};

        bindDocumentListeners();

        el.addEventListener('click', function(e) {
            if (e.target.closest('.cell-action-btn')) return;
            if (e.target.closest('a')) return;

            const cell = e.target.closest('.copyable');
            if (!cell || !el.contains(cell)) return;

            e.stopPropagation();
            activateCell(cell, options);
        });
    }

    window.FlowlordUtils = {
        escapeHtml: escapeHtml,
        escapeAttr: escapeAttr,
        escapeJsString: escapeJsString,
        copyToClipboard: copyToClipboard,
        copyableText: copyableText,
        enableCellActions: enableCellActions,
        showCopyFeedback: showCopyFeedback,
        deactivateCell: deactivateCell,
        copyCell: copyCell
    };
})();

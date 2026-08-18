// Common utility functions used across Flowlord pages
(function() {
    'use strict';

    // Escape HTML for safe display in innerHTML
    function escapeHtml(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    // Escape text for use in inline JavaScript attributes
    function escapeJsString(text) {
        if (text === null || text === undefined) return '';
        return String(text).replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"');
    }

    let dismissContextMenu = null;

    function hideContextMenu() {
        if (dismissContextMenu) {
            dismissContextMenu();
        }
    }

    // Context menu functionality
    function showContextMenu(event, text) {
        event.preventDefault();
        event.stopPropagation();

        hideContextMenu();

        const contextMenu = document.createElement('div');
        contextMenu.className = 'context-menu';

        const menuItem = document.createElement('div');
        menuItem.className = 'context-menu-item';
        menuItem.innerHTML = '📋 Copy';

        contextMenu.appendChild(menuItem);
        document.body.appendChild(contextMenu);

        // position:fixed uses viewport coords (clientX/Y), not document coords (pageX/Y)
        const menuWidth = contextMenu.offsetWidth;
        const menuHeight = contextMenu.offsetHeight;
        const left = Math.min(event.clientX, window.innerWidth - menuWidth - 8);
        const top = Math.min(event.clientY, window.innerHeight - menuHeight - 8);
        contextMenu.style.left = Math.max(8, left) + 'px';
        contextMenu.style.top = Math.max(8, top) + 'px';

        function onClick(e) {
            if (!contextMenu.contains(e.target)) {
                close();
            }
        }

        function onKey(e) {
            if (e.key === 'Escape') {
                close();
            }
        }

        function close() {
            contextMenu.remove();
            document.removeEventListener('click', onClick);
            document.removeEventListener('scroll', close, true);
            document.removeEventListener('keydown', onKey);
            if (dismissContextMenu === close) {
                dismissContextMenu = null;
            }
        }

        menuItem.addEventListener('click', function() {
            copyToClipboard(text);
            close();
        });

        dismissContextMenu = close;
        // Delay click-away so the opening gesture does not dismiss immediately
        setTimeout(function() {
            document.addEventListener('click', onClick);
        }, 100);
        document.addEventListener('scroll', close, true);
        document.addEventListener('keydown', onKey);
    }

    // Copy to clipboard functionality with enhanced feedback
    function copyToClipboard(text) {
        const targetElement = event ? event.target : document.activeElement;
        
        navigator.clipboard.writeText(text).then(function() {
            showCopyFeedback(targetElement, 'Copied!');
        }).catch(function(err) {
            console.error('Could not copy text: ', err);
            // Fallback for older browsers
            const textArea = document.createElement('textarea');
            textArea.value = text;
            textArea.style.position = 'fixed';
            textArea.style.left = '-999999px';
            textArea.style.top = '-999999px';
            document.body.appendChild(textArea);
            textArea.focus();
            textArea.select();
            try {
                document.execCommand('copy');
                showCopyFeedback(targetElement, 'Copied!');
            } catch (err) {
                console.error('Fallback copy failed: ', err);
                showCopyFeedback(targetElement, 'Copy failed!', true);
            }
            document.body.removeChild(textArea);
        });
    }

    // Show copy feedback with animation
    function showCopyFeedback(element, message, isError) {
        isError = isError || false;
        
        // Remove any existing feedback
        const existingFeedback = document.querySelector('.copy-feedback');
        if (existingFeedback) {
            existingFeedback.remove();
        }
        
        // Create feedback element
        const feedback = document.createElement('div');
        feedback.className = 'copy-feedback';
        feedback.textContent = message;
        feedback.style.backgroundColor = isError ? '#dc3545' : '#28a745';
        
        // Position feedback relative to the element
        const rect = element.getBoundingClientRect();
        feedback.style.position = 'fixed';
        feedback.style.left = (rect.left + rect.width / 2) + 'px';
        feedback.style.top = (rect.top - 10) + 'px';
        feedback.style.transform = 'translateX(-50%)';
        
        document.body.appendChild(feedback);
        
        // Remove feedback after animation
        setTimeout(() => {
            if (feedback.parentNode) {
                feedback.remove();
            }
        }, 2000);
    }

    function copyableText(cell) {
        if (!cell) return '';
        return (cell.getAttribute('data-full-text') || cell.textContent || '').trim();
    }

    // Right-click copy for any .copyable cell. Call once per table body (or table).
    function enableCopyableCells(root) {
        const el = typeof root === 'string' ? document.querySelector(root) : root;
        if (!el) return;

        el.addEventListener('contextmenu', function(e) {
            const cell = e.target.closest('.copyable');
            if (!cell || !el.contains(cell)) return;
            e.preventDefault();
            e.stopPropagation();
            showContextMenu(e, copyableText(cell));
        });
    }

    // Toggle field expansion
    function toggleField(element, fullText) {
        // Prevent event bubbling to avoid conflicts with sorting
        if (event) {
            event.stopPropagation();
        }
        
        if (element.classList.contains('expanded')) {
            // Collapse the field
            element.classList.remove('expanded');
            element.classList.add('truncated');
            // Reset to truncated text if available in data attribute
            const truncatedText = element.getAttribute('data-truncated-text');
            if (truncatedText) {
                element.textContent = truncatedText;
            }
        } else {
            // Expand the field
            element.classList.remove('truncated');
            element.classList.add('expanded');
            element.textContent = fullText;
        }
    }

    // Export to global scope
    window.FlowlordUtils = {
        escapeHtml: escapeHtml,
        escapeJsString: escapeJsString,
        showContextMenu: showContextMenu,
        copyToClipboard: copyToClipboard,
        copyableText: copyableText,
        enableCopyableCells: enableCopyableCells,
        showCopyFeedback: showCopyFeedback,
        toggleField: toggleField
    };
})();

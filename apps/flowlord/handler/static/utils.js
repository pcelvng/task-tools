// Common utility functions used across Flowlord pages
(function() {
    'use strict';

    // Context menu functionality
    function showContextMenu(event, text) {
        // Remove any existing context menu
        const existingMenu = document.querySelector('.context-menu');
        if (existingMenu) {
            existingMenu.remove();
        }
        
        // Create context menu
        const contextMenu = document.createElement('div');
        contextMenu.className = 'context-menu';
        contextMenu.innerHTML = `
            <div class="context-menu-item" onclick="window.FlowlordUtils.copyToClipboard('${escapeHtml(text)}')">
                📋 Copy
            </div>
        `;
        
        // Position the context menu
        contextMenu.style.left = event.pageX + 'px';
        contextMenu.style.top = event.pageY + 'px';
        
        document.body.appendChild(contextMenu);
        
        // Close context menu when clicking elsewhere
        const closeMenu = (e) => {
            if (!contextMenu.contains(e.target)) {
                contextMenu.remove();
                document.removeEventListener('click', closeMenu);
            }
        };
        
        setTimeout(() => {
            document.addEventListener('click', closeMenu);
        }, 100);
    }

    // Escape HTML for safe insertion
    function escapeHtml(text) {
        return text.replace(/'/g, "\\'").replace(/"/g, '\\"');
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
    function showCopyFeedback(element, message, isError = false) {
        // Remove any existing feedback
        const existingFeedback = element.querySelector('.copy-feedback');
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
        
        element.appendChild(feedback);
        
        // Remove feedback after animation
        setTimeout(() => {
            if (feedback.parentNode) {
                feedback.remove();
            }
        }, 2000);
    }

    // Toggle field expansion
    function toggleField(element, fullText) {
        if (element.classList.contains('truncated')) {
            element.classList.remove('truncated');
            element.classList.add('expanded');
            element.textContent = fullText;
        } else {
            element.classList.add('truncated');
            element.classList.remove('expanded');
            // Reset to truncated text if available in data attribute
            const truncatedText = element.getAttribute('data-truncated-text');
            if (truncatedText) {
                element.textContent = truncatedText;
            }
        }
    }

    // Export to global scope
    window.FlowlordUtils = {
        showContextMenu: showContextMenu,
        copyToClipboard: copyToClipboard,
        showCopyFeedback: showCopyFeedback,
        toggleField: toggleField
    };
})();


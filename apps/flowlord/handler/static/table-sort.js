// Reusable table sorting functionality
(function() {
    'use strict';

    /**
     * Initialize table sorting for a given table
     * @param {string} tableId - The ID of the table element
     * @param {Object} options - Configuration options
     * @param {Function} options.onSort - Callback when sort changes (column, direction)
     * @param {boolean} options.persistToUrl - Whether to persist sort state to URL (default: true)
     * @param {Object} options.columnTypes - Map of column names to types ('string', 'number', 'date')
     */
    function init(tableId, options) {
        const table = document.getElementById(tableId);
        if (!table) return null;

        const tbody = table.querySelector('tbody');
        const headers = table.querySelectorAll('th.sortable');
        
        if (!tbody || headers.length === 0) return null;

        options = options || {};
        const persistToUrl = options.persistToUrl !== false;
        const columnTypes = options.columnTypes || {};
        const onSort = options.onSort || null;

        let currentSort = { column: null, direction: 'asc' };

        // Get URL parameters
        function getUrlParams() {
            const urlParams = new URLSearchParams(window.location.search);
            return {
                sort: urlParams.get('sort') || '',
                direction: urlParams.get('direction') || 'asc'
            };
        }

        // Update URL with sort parameters
        function updateUrl(column, direction) {
            if (!persistToUrl) return;
            
            const url = new URL(window.location);
            
            if (column) {
                url.searchParams.set('sort', column);
                url.searchParams.set('direction', direction);
            } else {
                url.searchParams.delete('sort');
                url.searchParams.delete('direction');
            }
            
            window.history.replaceState({}, '', url.toString());
        }

        // Perform the sort
        function sortTable(column, direction) {
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const columnIndex = Array.from(headers).findIndex(th => th.dataset.sort === column);
            
            if (columnIndex === -1) return;

            const columnType = columnTypes[column] || 'string';

            rows.sort((a, b) => {
                const aCell = a.cells[columnIndex];
                const bCell = b.cells[columnIndex];
                
                if (!aCell || !bCell) return 0;
                
                const aVal = aCell.textContent.trim();
                const bVal = bCell.textContent.trim();
                
                let comparison = 0;
                
                switch (columnType) {
                    case 'date':
                    case 'datetime':
                        const aDate = new Date(aVal);
                        const bDate = new Date(bVal);
                        if (!isNaN(aDate.getTime()) && !isNaN(bDate.getTime())) {
                            comparison = aDate - bDate;
                        } else {
                            comparison = aVal.localeCompare(bVal);
                        }
                        break;
                        
                    case 'number':
                        const aNum = parseFloat(aVal);
                        const bNum = parseFloat(bVal);
                        if (!isNaN(aNum) && !isNaN(bNum)) {
                            comparison = aNum - bNum;
                        } else {
                            comparison = aVal.localeCompare(bVal);
                        }
                        break;
                        
                    default: // string
                        // Try to parse as numbers first
                        const aNumeric = parseFloat(aVal);
                        const bNumeric = parseFloat(bVal);
                        if (!isNaN(aNumeric) && !isNaN(bNumeric)) {
                            comparison = aNumeric - bNumeric;
                        } else {
                            comparison = aVal.localeCompare(bVal);
                        }
                }
                
                return direction === 'asc' ? comparison : -comparison;
            });
            
            // Clear tbody and re-append sorted rows
            tbody.innerHTML = '';
            rows.forEach(row => tbody.appendChild(row));
            
            currentSort = { column, direction };
        }

        // Update sort indicators on headers
        function updateSortIndicators(activeColumn, direction) {
            headers.forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
                if (th.dataset.sort === activeColumn) {
                    th.classList.add(direction === 'asc' ? 'sort-asc' : 'sort-desc');
                }
            });
        }

        // Handle header click
        function handleHeaderClick(e) {
            const header = e.target.closest('th.sortable');
            if (!header) return;
            
            const column = header.dataset.sort;
            let direction = 'asc';
            
            if (currentSort.column === column) {
                direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            }
            
            sortTable(column, direction);
            updateSortIndicators(column, direction);
            updateUrl(column, direction);
            
            if (onSort) {
                onSort(column, direction);
            }
        }

        // Add click listeners to headers
        headers.forEach(header => {
            header.addEventListener('click', handleHeaderClick);
        });

        // Initialize from URL if enabled
        if (persistToUrl) {
            const params = getUrlParams();
            if (params.sort) {
                sortTable(params.sort, params.direction);
                updateSortIndicators(params.sort, params.direction);
            }
        }

        // Return API for programmatic control
        return {
            sort: function(column, direction) {
                sortTable(column, direction || 'asc');
                updateSortIndicators(column, direction || 'asc');
                updateUrl(column, direction || 'asc');
            },
            getCurrentSort: function() {
                return { ...currentSort };
            },
            refresh: function() {
                if (currentSort.column) {
                    sortTable(currentSort.column, currentSort.direction);
                }
            }
        };
    }

    // Export to global scope
    window.FlowlordTableSort = {
        init: init
    };
})();

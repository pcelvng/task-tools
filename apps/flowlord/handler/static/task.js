// Task page functionality
(function() {
    'use strict';

    // Initialize task page with configuration
    function initTaskPage(config) {
        const table = document.getElementById('taskTable');
        if (!table) {
            return;
        }
        
        const tbody = table.querySelector('tbody');
        const headers = table.querySelectorAll('th.sortable');
        
        let currentSort = { column: null, direction: 'asc' };

        // Get URL parameters
        function getUrlParams() {
            const urlParams = new URLSearchParams(window.location.search);
            return {
                date: urlParams.get('date') || '',
                type: urlParams.get('type') || '',
                job: urlParams.get('job') || '',
                result: urlParams.get('result') || '',
                sort: urlParams.get('sort') || '',
                direction: urlParams.get('direction') || 'asc'
            };
        }

        // Update URL with new parameters
        function updateUrl(date, type, job, result, sort, direction) {
            const url = new URL(window.location);
            
            if (date) url.searchParams.set('date', date);
            else url.searchParams.delete('date');
            
            if (type) url.searchParams.set('type', type);
            else url.searchParams.delete('type');
            
            if (job) url.searchParams.set('job', job);
            else url.searchParams.delete('job');
            
            if (result) url.searchParams.set('result', result);
            else url.searchParams.delete('result');
            
            if (sort) {
                url.searchParams.set('sort', sort);
                url.searchParams.set('direction', direction);
            } else {
                url.searchParams.delete('sort');
                url.searchParams.delete('direction');
            }
            
            // Reload page with new URL
            window.location.href = url.toString();
        }

        // Initialize sorting from URL
        function initializeSorting() {
            const params = getUrlParams();
            
            if (params.sort) {
                currentSort = { column: params.sort, direction: params.direction };
                updateSortIndicators(params.sort, params.direction);
            }
        }

        function sortTable(column, direction) {
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const columnIndex = Array.from(headers).findIndex(th => th.dataset.sort === column);
            
            rows.sort((a, b) => {
                const aVal = a.cells[columnIndex].textContent.trim();
                const bVal = b.cells[columnIndex].textContent.trim();
                
                let comparison = 0;
                
                // Check if this is a datetime column
                if (column === 'created' || column === 'started' || column === 'ended') {
                    const aDate = new Date(aVal);
                    const bDate = new Date(bVal);
                    
                    if (!isNaN(aDate.getTime()) && !isNaN(bDate.getTime())) {
                        comparison = aDate - bDate;
                    } else {
                        comparison = aVal.localeCompare(bVal);
                    }
                } else if (column === 'duration') {
                    // Parse duration strings like "1h2m3s" or "N/A"
                    if (aVal === 'N/A' && bVal === 'N/A') comparison = 0;
                    else if (aVal === 'N/A') comparison = 1;
                    else if (bVal === 'N/A') comparison = -1;
                    else comparison = aVal.localeCompare(bVal);
                } else {
                    // Try to parse as numbers first
                    const aNum = parseFloat(aVal);
                    const bNum = parseFloat(bVal);
                    
                    if (!isNaN(aNum) && !isNaN(bNum)) {
                        comparison = aNum - bNum;
                    } else {
                        comparison = aVal.localeCompare(bVal);
                    }
                }
                
                return direction === 'asc' ? comparison : -comparison;
            });
            
            // Clear tbody and re-append sorted rows
            tbody.innerHTML = '';
            rows.forEach(row => tbody.appendChild(row));
        }

        function updateSortIndicators(activeColumn, direction) {
            headers.forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
                if (th.dataset.sort === activeColumn) {
                    th.classList.add(direction === 'asc' ? 'sort-asc' : 'sort-desc');
                }
            });
        }

        // Column sorting event listeners
        headers.forEach(header => {
            header.addEventListener('click', function() {
                const column = this.dataset.sort;
                let direction = 'asc';
                
                if (currentSort.column === column) {
                    direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
                }
                
                currentSort = { column, direction };
                const params = getUrlParams();
                updateUrl(params.date, params.type, params.job, params.result, column, direction);
            });
        });

        // Initialize filters
        initializeFilters(config);

        // Event delegation for expand/collapse on click
        if (tbody) {
            tbody.addEventListener('click', function(e) {
                const cell = e.target.closest('.expandable');
                if (cell) {
                    e.stopPropagation();
                    cell.classList.toggle('expanded');
                }
            });
        }

        // Event delegation for copy on double-click
        if (tbody) {
            tbody.addEventListener('dblclick', function(e) {
                const cell = e.target.closest('.expandable');
                if (cell) {
                    e.stopPropagation();
                    e.preventDefault();
                    window.FlowlordUtils.copyToClipboard(cell.textContent.trim());
                }
            });
        }

        // Event delegation for context menu  
        if (tbody) {
            tbody.addEventListener('contextmenu', function(e) {
                const cell = e.target.closest('.expandable');
                if (cell) {
                    e.preventDefault();
                    e.stopPropagation();
                    window.FlowlordUtils.showContextMenu(e, cell.textContent.trim());
                }
            });
        }

        // Initialize the page
        initializeSorting();
    }

    // Initialize responsive filters
    function initializeFilters(config) {
        const typeFilter = document.getElementById('typeFilter');
        const jobFilter = document.getElementById('jobFilter');
        const table = document.getElementById('taskTable');
        
        if (!table || !config) return;
        
        const taskTypes = config.taskTypes || [];
        const jobMap = new Map(config.jobsByType || []);
        const currentType = config.currentType || "";
        const currentJob = config.currentJob || "";
        
        // Populate task type dropdown from server data
        taskTypes.forEach(type => {
            const option = document.createElement('option');
            option.value = type;
            option.textContent = type;
            typeFilter.appendChild(option);
        });
        
        // Populate job dropdown based on current type selection
        if (currentType && jobMap.has(currentType)) {
            const jobs = jobMap.get(currentType);
            jobs.forEach(job => {
                const option = document.createElement('option');
                option.value = job;
                option.textContent = job;
                if (job === currentJob) {
                    option.selected = true;
                }
                jobFilter.appendChild(option);
            });
        }
        
        // Set current filter values from URL
        if (currentType) {
            typeFilter.value = currentType;
        }
        
        // Handle task type change - update job dropdown and apply filter
        typeFilter.addEventListener('change', function() {
            const selectedType = this.value;
            const jobOptions = jobFilter.querySelectorAll('option:not([value=""])');
            jobOptions.forEach(option => option.remove());
            jobFilter.value = ''; // Clear job selection
            
            if (selectedType && jobMap.has(selectedType)) {
                const jobs = jobMap.get(selectedType);
                jobs.forEach(job => {
                    const option = document.createElement('option');
                    option.value = job;
                    option.textContent = job;
                    jobFilter.appendChild(option);
                });
            }
            
            // Apply filter by reloading page
            applyFiltersWithResultReset();
        });
        
        // Handle job change - reload page with filter
        jobFilter.addEventListener('change', function() {
            applyFilters();
        });
    }
    
    // Apply filters by reloading page with query parameters
    function applyFilters() {
        const typeFilter = document.getElementById('typeFilter');
        const jobFilter = document.getElementById('jobFilter');
        
        const url = new URL(window.location);
        url.searchParams.delete('page'); // Reset to page 1 when filtering
        
        const selectedType = typeFilter ? typeFilter.value : '';
        const selectedJob = jobFilter ? jobFilter.value : '';
        
        if (selectedType) {
            url.searchParams.set('type', selectedType);
        } else {
            url.searchParams.delete('type');
        }
        
        if (selectedJob) {
            url.searchParams.set('job', selectedJob);
        } else {
            url.searchParams.delete('job');
        }
        
        window.location.href = url.toString();
    }
    
    // Apply filters and reset result filter (for task type changes)
    function applyFiltersWithResultReset() {
        const typeFilter = document.getElementById('typeFilter');
        const jobFilter = document.getElementById('jobFilter');
        
        const url = new URL(window.location);
        url.searchParams.delete('page'); // Reset to page 1 when filtering
        url.searchParams.delete('result'); // Reset result filter to show all results
        
        const selectedType = typeFilter ? typeFilter.value : '';
        const selectedJob = jobFilter ? jobFilter.value : '';
        
        if (selectedType) {
            url.searchParams.set('type', selectedType);
        } else {
            url.searchParams.delete('type');
        }
        
        if (selectedJob) {
            url.searchParams.set('job', selectedJob);
        } else {
            url.searchParams.delete('job');
        }
        
        window.location.href = url.toString();
    }

    // Clear all filters
    window.clearFilters = function() {
        const url = new URL(window.location);
        url.searchParams.delete('id');
        url.searchParams.delete('type');
        url.searchParams.delete('job');
        url.searchParams.delete('result');
        url.searchParams.delete('page');
        window.location.href = url.toString();
    };

    // Filter by result type using stat-cards
    window.filterByResult = function(resultType) {
        const url = new URL(window.location);
        url.searchParams.delete('page'); // Reset to page 1
        
        // Reset task type and job filters when clicking on stat cards
        url.searchParams.delete('type');
        url.searchParams.delete('job');
        
        if (resultType === 'all') {
            url.searchParams.delete('result');
        } else {
            url.searchParams.set('result', resultType);
        }
        
        window.location.href = url.toString();
    };

    // Toggle collapsible section
    window.toggleCollapsible = function(sectionId) {
        const content = document.getElementById(sectionId + '-content');
        const toggle = document.getElementById(sectionId + '-toggle');
        
        if (content.classList.contains('collapsed')) {
            content.classList.remove('collapsed');
            toggle.classList.add('expanded');
        } else {
            content.classList.add('collapsed');
            toggle.classList.remove('expanded');
        }
    };

    // Export to global scope
    window.FlowlordTask = {
        init: initTaskPage
    };
})();


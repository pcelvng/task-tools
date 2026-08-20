// Task page functionality
(function() {
    'use strict';

    // Initialize task page with configuration
    function initTaskPage(config) {
        // Always initialize filters, even if there's no table
        initializeFilters(config);
        
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

        // Update URL with new parameters (server applies sort on reload)
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

            // Reset to first page when sort changes
            url.searchParams.delete('page');

            // Keep viewport on the table after reload
            try {
                sessionStorage.setItem('flowlordTaskScrollY', String(window.scrollY));
            } catch (e) { /* ignore quota / private mode */ }
            
            window.location.href = url.toString();
        }

        // Restore scroll position after a sort reload
        function restoreScrollAfterSort() {
            let y = null;
            try {
                y = sessionStorage.getItem('flowlordTaskScrollY');
                sessionStorage.removeItem('flowlordTaskScrollY');
            } catch (e) {
                return;
            }
            if (y === null) return;
            const top = parseInt(y, 10);
            if (!isNaN(top)) {
                window.scrollTo(0, top);
            }
        }

        // Initialize sorting state from URL (indicators come from server-rendered classes)
        function initializeSorting() {
            const params = getUrlParams();
            
            if (params.sort) {
                currentSort = { column: params.sort, direction: params.direction };
            }
        }

        // Column sorting event listeners — reload with sort params for server-side ordering
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

        // Event delegation for expand/collapse on click
        if (tbody) {
            tbody.addEventListener('click', function(e) {
                const cell = e.target.closest('.expandable');
                if (cell) {
                    e.stopPropagation();
                    cell.classList.toggle('expanded');
                }
            });
            window.FlowlordUtils.enableCopyableCells(tbody);
        }

        // Initialize the page
        initializeSorting();
        restoreScrollAfterSort();
    }

    // Initialize responsive filters
    function initializeFilters(config) {
        const typeFilter = document.getElementById('typeFilter');
        const jobFilter = document.getElementById('jobFilter');
        
        if (!typeFilter || !jobFilter || !config) return;
        
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

        // Handle result change - reload page with filter
        const resultFilter = document.getElementById('resultFilter');
        if (resultFilter) {
            resultFilter.addEventListener('change', function() {
                applyFilters();
            });
        }
    }
    
    // Apply filters by reloading page with query parameters
    function applyFilters() {
        const typeFilter = document.getElementById('typeFilter');
        const jobFilter = document.getElementById('jobFilter');
        const resultFilter = document.getElementById('resultFilter');
        
        const url = new URL(window.location);
        url.searchParams.delete('page'); // Reset to page 1 when filtering
        
        const selectedType = typeFilter ? typeFilter.value : '';
        const selectedJob = jobFilter ? jobFilter.value : '';
        const selectedResult = resultFilter ? resultFilter.value : '';
        
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
        
        if (selectedResult) {
            url.searchParams.set('result', selectedResult);
        } else {
            url.searchParams.delete('result');
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


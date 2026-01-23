// Backload form functionality
(function() {
    'use strict';

    // Module state
    let allPhases = [];
    let currentPhase = null;
    let previewTasks = [];
    let searchTimeout = null;
    let selectedDropdownIndex = -1;

    // DOM element references (cached on init)
    const elements = {};

    // Initialize the backload form
    function init(phasesData, apiEndpoint) {
        allPhases = phasesData || [];
        
        // Cache DOM elements
        elements.taskSearch = document.getElementById('taskSearch');
        elements.taskDropdown = document.getElementById('taskDropdown');
        elements.taskSelect = document.getElementById('taskSelect');
        elements.workflowFilter = document.getElementById('workflowFilter');
        elements.jobSelect = document.getElementById('jobSelect');
        elements.templateSection = document.getElementById('templateSection');
        elements.templateDisplay = document.getElementById('templateDisplay');
        elements.ruleDisplay = document.getElementById('ruleDisplay');
        elements.metaSection = document.getElementById('metaSection');
        elements.metaFieldsContainer = document.getElementById('metaFieldsContainer');
        elements.metaFileSection = document.getElementById('metaFileSection');
        elements.metaFileInput = document.getElementById('metaFileInput');
        elements.previewBtn = document.getElementById('previewBtn');
        elements.executeBtn = document.getElementById('executeBtn');
        elements.resetBtn = document.getElementById('resetBtn');
        elements.previewSection = document.getElementById('previewSection');
        elements.previewStatus = document.getElementById('previewStatus');
        elements.previewTableBody = document.getElementById('previewTableBody');
        elements.previewCount = document.getElementById('previewCount');
        elements.executionSection = document.getElementById('executionSection');
        elements.executionStatus = document.getElementById('executionStatus');
        elements.requestBodySection = document.getElementById('requestBodySection');
        elements.requestBodyDisplay = document.getElementById('requestBodyDisplay');
        elements.fromDate = document.getElementById('fromDate');
        elements.toDate = document.getElementById('toDate');
        elements.atDate = document.getElementById('atDate');
        elements.bySelect = document.getElementById('bySelect');
        elements.singleDateInput = document.getElementById('singleDateInput');
        elements.dateRangeInputs = document.getElementById('dateRangeInputs');

        // Store API endpoint
        elements.apiEndpoint = apiEndpoint || '/backload';

        // Setup event listeners
        setupEventListeners();

        // Initialize date inputs with today's date
        initializeDates();
    }

    // Get unique tasks from phases
    function getUniqueTasks(workflowFilterValue) {
        const taskSet = new Set();
        allPhases.forEach(p => {
            if (!workflowFilterValue || p.workflow === workflowFilterValue) {
                taskSet.add(p.task);
            }
        });
        return Array.from(taskSet).sort();
    }

    // Setup all event listeners
    function setupEventListeners() {
        // Date mode toggle
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            btn.addEventListener('click', handleDateModeToggle);
        });

        // Task search
        elements.taskSearch.addEventListener('input', handleTaskSearchInput);
        elements.taskSearch.addEventListener('focus', handleTaskSearchFocus);
        elements.taskSearch.addEventListener('keydown', handleTaskSearchKeydown);

        // Task dropdown
        elements.taskDropdown.addEventListener('click', handleDropdownClick);

        // Close dropdown when clicking outside
        document.addEventListener('click', handleDocumentClick);

        // Workflow filter
        elements.workflowFilter.addEventListener('change', handleWorkflowFilterChange);

        // Job selection
        elements.jobSelect.addEventListener('change', handleJobSelectChange);

        // Date inputs
        ['fromDate', 'toDate', 'atDate'].forEach(id => {
            document.getElementById(id).addEventListener('change', updatePreviewButton);
        });

        // Buttons
        elements.previewBtn.addEventListener('click', handlePreviewClick);
        elements.executeBtn.addEventListener('click', handleExecuteClick);
        elements.resetBtn.addEventListener('click', handleResetClick);
    }

    // Date mode toggle handler
    function handleDateModeToggle() {
        document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');
        
        if (this.dataset.mode === 'range') {
            elements.dateRangeInputs.style.display = 'block';
            elements.singleDateInput.style.display = 'none';
        } else {
            elements.dateRangeInputs.style.display = 'none';
            elements.singleDateInput.style.display = 'block';
        }
        updatePreviewButton();
    }

    // Task search input handler
    function handleTaskSearchInput() {
        clearTimeout(searchTimeout);
        const query = this.value.trim();
        
        searchTimeout = setTimeout(() => {
            showTaskDropdown(query);
        }, 100);
    }

    // Task search focus handler
    function handleTaskSearchFocus() {
        this.value = '';
        elements.taskSelect.value = '';
        selectedDropdownIndex = -1;
        showTaskDropdown('');
    }

    // Task search keydown handler for keyboard navigation
    function handleTaskSearchKeydown(e) {
        const items = elements.taskDropdown.querySelectorAll('.search-dropdown-item');
        if (items.length === 0) return;

        switch(e.key) {
            case 'ArrowDown':
                e.preventDefault();
                selectedDropdownIndex = Math.min(selectedDropdownIndex + 1, items.length - 1);
                updateDropdownSelection(items);
                break;
            case 'ArrowUp':
                e.preventDefault();
                selectedDropdownIndex = Math.max(selectedDropdownIndex - 1, 0);
                updateDropdownSelection(items);
                break;
            case 'Enter':
                e.preventDefault();
                if (selectedDropdownIndex >= 0 && items[selectedDropdownIndex]) {
                    const task = items[selectedDropdownIndex].dataset.task;
                    selectTask(task);
                }
                break;
            case 'Escape':
                e.preventDefault();
                elements.taskDropdown.style.display = 'none';
                selectedDropdownIndex = -1;
                break;
        }
    }

    // Update dropdown selection highlight
    function updateDropdownSelection(items) {
        items.forEach((item, index) => {
            if (index === selectedDropdownIndex) {
                item.classList.add('selected');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('selected');
            }
        });
    }

    // Select a task
    function selectTask(task) {
        elements.taskSearch.value = task;
        elements.taskSelect.value = task;
        elements.taskDropdown.style.display = 'none';
        selectedDropdownIndex = -1;
        
        // Set workflow filter to the task's workflow if not already filtered
        if (!elements.workflowFilter.value) {
            const phase = allPhases.find(p => p.task === task);
            if (phase && phase.workflow) {
                elements.workflowFilter.value = phase.workflow;
            }
        }
        
        onTaskSelected(task);
    }

    // Show task dropdown with optional filtering
    function showTaskDropdown(query) {
        const workflow = elements.workflowFilter.value;
        const tasks = getUniqueTasks(workflow);
        const matches = query 
            ? tasks.filter(t => t.toLowerCase().includes(query.toLowerCase()))
            : tasks;
        
        selectedDropdownIndex = -1;
        
        if (matches.length > 0) {
            elements.taskDropdown.innerHTML = matches.map(task => 
                `<div class="search-dropdown-item" data-task="${escapeHtml(task)}">${query ? highlightMatch(task, query) : escapeHtml(task)}</div>`
            ).join('');
            elements.taskDropdown.style.display = 'block';
        } else {
            elements.taskDropdown.innerHTML = '<div class="search-dropdown-empty">No matching tasks</div>';
            elements.taskDropdown.style.display = 'block';
        }
    }

    // Highlight matching text in search results
    function highlightMatch(text, query) {
        const idx = text.toLowerCase().indexOf(query.toLowerCase());
        if (idx === -1) return escapeHtml(text);
        return escapeHtml(text.slice(0, idx)) + '<strong>' + escapeHtml(text.slice(idx, idx + query.length)) + '</strong>' + escapeHtml(text.slice(idx + query.length));
    }

    // Handle dropdown item click
    function handleDropdownClick(e) {
        const item = e.target.closest('.search-dropdown-item');
        if (item) {
            const task = item.dataset.task;
            selectTask(task);
        }
    }

    // Close dropdown when clicking outside
    function handleDocumentClick(e) {
        if (!elements.taskSearch.contains(e.target) && !elements.taskDropdown.contains(e.target)) {
            elements.taskDropdown.style.display = 'none';
            selectedDropdownIndex = -1;
        }
    }

    // Workflow filter change handler
    function handleWorkflowFilterChange() {
        elements.taskSearch.value = '';
        elements.taskSelect.value = '';
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        hideTemplateInfo();
        updatePreviewButton();
    }

    // Handle task selection
    function onTaskSelected(task) {
        const workflow = elements.workflowFilter.value;
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        hideTemplateInfo();
        
        // Find phases matching this task (optionally filtered by workflow)
        const phases = allPhases.filter(p => 
            p.task === task && (!workflow || p.workflow === workflow)
        );
        const jobs = [...new Set(phases.map(p => p.job).filter(j => j))];
        
        if (jobs.length > 0) {
            jobs.sort().forEach(job => {
                const option = document.createElement('option');
                option.value = job;
                option.textContent = job;
                elements.jobSelect.appendChild(option);
            });
            elements.jobSelect.disabled = false;
        } else {
            // No job needed, use first phase
            const phase = phases[0];
            if (phase) {
                showTemplateInfo(phase);
            }
        }
        updatePreviewButton();
    }

    // Job selection change handler
    function handleJobSelectChange() {
        const task = elements.taskSelect.value;
        const job = this.value;
        const workflow = elements.workflowFilter.value;
        
        const phase = allPhases.find(p => 
            p.task === task && 
            (p.job === job || (!job && !p.job)) &&
            (!workflow || p.workflow === workflow)
        );
        if (phase) {
            showTemplateInfo(phase);
        }
        updatePreviewButton();
    }

    // Format rule string for better readability
    function formatRule(str) {
        if (!str) return '(no rule)';
        return str.split('&').join('\n');
    }

    // Show template information and detect meta fields
    function showTemplateInfo(phase) {
        currentPhase = phase;
        elements.templateSection.style.display = 'block';
        elements.templateDisplay.textContent = phase.template || '(no template)';
        elements.ruleDisplay.textContent = formatRule(phase.rule);
        
        // Parse template for meta fields
        const metaRegex = /\{meta:(\w+)\}/g;
        const metaKeys = [];
        let match;
        while ((match = metaRegex.exec(phase.template)) !== null) {
            if (!metaKeys.includes(match[1])) {
                metaKeys.push(match[1]);
            }
        }
        
        // Check if rule has meta-file
        const hasMetaFile = phase.rule && phase.rule.includes('meta-file=');
        
        // Show meta fields section if template has meta placeholders and no meta-file in rule
        if (metaKeys.length > 0 && !hasMetaFile) {
            elements.metaSection.style.display = 'block';
            elements.metaFieldsContainer.innerHTML = '';
            
            metaKeys.forEach(key => {
                const formGroup = document.createElement('div');
                formGroup.className = 'form-group';
                formGroup.innerHTML = `
                    <label for="meta_${key}">{meta:${key}}</label>
                    <input type="text" id="meta_${key}" class="form-control meta-input" 
                           data-meta-key="${key}" placeholder="Enter values (comma-separated for multiple)">
                    <small class="form-hint">Comma-separated values create multiple tasks</small>
                `;
                elements.metaFieldsContainer.appendChild(formGroup);
            });
        } else {
            elements.metaSection.style.display = 'none';
        }
        
        // Show meta file section if rule has meta-file
        if (hasMetaFile) {
            elements.metaFileSection.style.display = 'block';
            const metaFileMatch = phase.rule.match(/meta-file=([^&]+)/);
            if (metaFileMatch) {
                elements.metaFileInput.value = metaFileMatch[1];
            }
        } else {
            elements.metaFileSection.style.display = 'none';
            elements.metaFileInput.value = '';
        }
    }

    // Hide template information
    function hideTemplateInfo() {
        currentPhase = null;
        elements.templateSection.style.display = 'none';
        elements.metaSection.style.display = 'none';
        elements.metaFileSection.style.display = 'none';
        elements.previewSection.style.display = 'none';
        elements.executionSection.style.display = 'none';
        elements.executeBtn.style.display = 'none';
        elements.requestBodySection.style.display = 'none';
        
        elements.metaFieldsContainer.innerHTML = '';
        elements.metaFileInput.value = '';
    }

    // Get current date mode from toggle
    function getDateMode() {
        const activeBtn = document.querySelector('.toggle-btn.active');
        return activeBtn ? activeBtn.dataset.mode : 'range';
    }

    // Update preview button state
    function updatePreviewButton() {
        const task = elements.taskSelect.value;
        const dateMode = getDateMode();
        let hasDate = false;
        
        if (dateMode === 'range') {
            hasDate = elements.fromDate.value || elements.toDate.value;
        } else {
            hasDate = elements.atDate.value;
        }
        
        elements.previewBtn.disabled = !task || !hasDate;
    }

    // Build request object
    function buildRequest(execute) {
        const dateMode = getDateMode();
        const request = {
            Task: elements.taskSelect.value,
        };
        
        if (execute) {
            request.Execute = true;
        }
        
        const job = elements.jobSelect.value;
        if (job) {
            request.Job = job;
        }
        
        const by = elements.bySelect.value;
        if (by && by !== 'day') {
            request.By = by;
        }
        
        if (dateMode === 'range') {
            if (elements.fromDate.value) request.From = elements.fromDate.value;
            if (elements.toDate.value) request.To = elements.toDate.value;
        } else {
            if (elements.atDate.value) request.At = elements.atDate.value;
        }
        
        // Collect meta fields
        if (elements.metaSection.style.display !== 'none') {
            const metaInputs = document.querySelectorAll('.meta-input');
            if (metaInputs.length > 0) {
                const meta = {};
                metaInputs.forEach(input => {
                    const key = input.dataset.metaKey;
                    const value = input.value.trim();
                    if (value) {
                        meta[key] = value.split(',').map(v => v.trim());
                    }
                });
                if (Object.keys(meta).length > 0) {
                    request.meta = meta;
                }
            }
        }
        
        // Collect meta file
        if (elements.metaFileSection.style.display !== 'none') {
            const metaFile = elements.metaFileInput.value.trim();
            if (metaFile) {
                request['meta-file'] = metaFile;
            }
        }
        
        return request;
    }

    // Set button loading state
    function setButtonLoading(btn, loading, originalText) {
        if (loading) {
            btn.disabled = true;
            btn.classList.add('btn-loading');
            btn.innerHTML = '<span class="loading-spinner"></span> Loading...';
        } else {
            btn.disabled = false;
            btn.classList.remove('btn-loading');
            btn.textContent = originalText;
        }
    }

    // Preview button click handler
    async function handlePreviewClick() {
        const request = buildRequest(false);
        const requestBody = JSON.stringify(request, null, 2);
        
        elements.requestBodySection.style.display = 'block';
        elements.requestBodyDisplay.textContent = requestBody;
        
        setButtonLoading(elements.previewBtn, true, 'Preview (Dry Run)');
        
        try {
            const response = await fetch(elements.apiEndpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(request),
            });
            
            const responseText = await response.text();
            let data;
            
            try {
                data = JSON.parse(responseText);
            } catch (e) {
                throw new Error(responseText || 'Request failed');
            }
            
            if (!response.ok) {
                throw new Error(data.Status || responseText || 'Request failed');
            }
            
            previewTasks = data.Tasks || [];
            showPreviewResults(data);
            
        } catch (error) {
            elements.previewStatus.className = 'preview-status error';
            elements.previewStatus.textContent = 'Error: ' + error.message;
            elements.previewSection.style.display = 'block';
            elements.previewTableBody.innerHTML = '';
            elements.previewCount.textContent = '';
            elements.executeBtn.style.display = 'none';
        } finally {
            setButtonLoading(elements.previewBtn, false, 'Preview (Dry Run)');
            updatePreviewButton();
        }
    }

    // Execute button click handler
    async function handleExecuteClick() {
        if (!confirm('Are you sure you want to execute this backload? This will create ' + previewTasks.length + ' tasks.')) {
            return;
        }
        
        const request = buildRequest(true);
        elements.requestBodyDisplay.textContent = JSON.stringify(request, null, 2);
        
        setButtonLoading(elements.executeBtn, true, 'Execute Backload');
        
        try {
            const response = await fetch(elements.apiEndpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(request),
            });
            
            const responseText = await response.text();
            let data;
            
            try {
                data = JSON.parse(responseText);
            } catch (e) {
                throw new Error(responseText || 'Execution failed');
            }
            
            if (!response.ok) {
                throw new Error(data.Status || responseText || 'Execution failed');
            }
            
            elements.executionSection.style.display = 'block';
            elements.executionStatus.className = 'execution-status success';
            elements.executionStatus.innerHTML = `
                <strong>Success!</strong><br>
                ${escapeHtml(data.Status)}<br>
                Created ${data.Count} tasks.
            `;
            elements.executeBtn.style.display = 'none';
            
        } catch (error) {
            elements.executionSection.style.display = 'block';
            elements.executionStatus.className = 'execution-status error';
            elements.executionStatus.textContent = 'Error: ' + error.message;
        } finally {
            setButtonLoading(elements.executeBtn, false, 'Execute Backload');
        }
    }

    // Reset button click handler
    function handleResetClick() {
        elements.taskSearch.value = '';
        elements.taskSelect.value = '';
        elements.workflowFilter.value = '';
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        elements.fromDate.value = '';
        elements.toDate.value = '';
        elements.atDate.value = '';
        elements.bySelect.value = 'day';
        
        document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        document.querySelector('.toggle-btn[data-mode="single"]').classList.add('active');
        elements.singleDateInput.style.display = 'block';
        elements.dateRangeInputs.style.display = 'none';
        
        hideTemplateInfo();
        initializeDates();
        updatePreviewButton();
    }

    // Show preview results
    function showPreviewResults(data) {
        elements.previewSection.style.display = 'block';
        elements.previewStatus.className = 'preview-status info';
        elements.previewStatus.textContent = data.Status || 'Dry run complete';
        
        elements.previewTableBody.innerHTML = '';
        
        if (data.Tasks && data.Tasks.length > 0) {
            data.Tasks.forEach((task, index) => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td class="num-cell num-column">${index + 1}</td>
                    <td class="type-cell type-column">${escapeHtml(task.type || '')}</td>
                    <td class="job-cell job-column">${escapeHtml(task.job || '')}</td>
                    <td class="info-cell info-column expandable" title="Click to expand">${escapeHtml(task.info || '')}</td>
                    <td class="meta-cell meta-column expandable" title="Click to expand">${escapeHtml(task.meta || '')}</td>
                `;
                elements.previewTableBody.appendChild(row);
            });
            
            elements.previewCount.textContent = `Total tasks to be created: ${data.Count}`;
            elements.executeBtn.style.display = 'inline-block';
            elements.executeBtn.disabled = false;
        } else {
            elements.previewTableBody.innerHTML = '<tr><td colspan="5" class="no-tasks">No tasks would be created</td></tr>';
            elements.previewCount.textContent = '';
            elements.executeBtn.style.display = 'none';
        }
        
        // Add expand/collapse functionality to cells
        document.querySelectorAll('#previewTableBody .expandable').forEach(cell => {
            cell.addEventListener('click', function() {
                this.classList.toggle('expanded');
            });
        });
    }

    // Initialize date inputs with today's date
    function initializeDates() {
        const today = new Date().toISOString().split('T')[0];
        elements.fromDate.value = today;
        elements.toDate.value = today;
        elements.atDate.value = today;
        updatePreviewButton();
    }

    // Escape HTML for safe display
    function escapeHtml(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    // Export to global scope
    window.FlowlordBackload = {
        init: init
    };
})();

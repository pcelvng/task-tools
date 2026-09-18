// Backload form functionality
(function() {
    'use strict';

    // Module state
    let allPhases = [];
    let currentPhase = null;
    let selectedWorkflow = '';
    let previewTasks = [];
    let searchTimeout = null;
    let selectedDropdownIndex = -1;

    // DOM element references (cached on init)
    const elements = {};

    // DateTime picker instances
    let pickers = {
        at: null,
        from: null,
        to: null
    };

    function taskPageBase() {
        return window.location.pathname.includes('_preview') ? './task_preview.html' : '/web/task';
    }

    function findPhase(task, job, workflow) {
        return allPhases.find(p =>
            p.task === task &&
            (p.job === job || (!job && !p.job)) &&
            (!workflow || p.workflow === workflow)
        );
    }

    function setSelectedWorkflow(workflow) {
        selectedWorkflow = workflow || '';
    }

    function parseMetaString(metaStr) {
        if (window.FlowlordUtils && window.FlowlordUtils.parseMetaString) {
            return window.FlowlordUtils.parseMetaString(metaStr);
        }
        const out = {};
        if (!metaStr) return out;
        metaStr.split('&').forEach(pair => {
            const eq = pair.indexOf('=');
            if (eq === -1) return;
            const key = decodeURIComponent(pair.slice(0, eq));
            const val = decodeURIComponent(pair.slice(eq + 1));
            if (key) out[key] = val;
        });
        return out;
    }

    function taskCreatedDate(task) {
        if (task && task.created) {
            const d = task.created.slice(0, 10);
            if (/^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
        }
        return new Date().toISOString().split('T')[0];
    }

    function buildViewAllTasksHref(tasks) {
        const ids = (tasks || []).map(t => t.id).filter(Boolean);
        if (ids.length === 0) return '';
        const date = taskCreatedDate(tasks[0]);
        return taskPageBase() + '?date=' + encodeURIComponent(date) + '&id=' + ids.map(encodeURIComponent).join(',');
    }

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
        elements.workflowDisplay = document.getElementById('workflowDisplay');
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
        elements.previewResultsHeading = document.getElementById('previewResultsHeading');
        elements.previewStatus = document.getElementById('previewStatus');
        elements.previewTableBody = document.getElementById('previewTableBody');
        elements.previewIdHeader = document.getElementById('previewIdHeader');
        elements.previewCount = document.getElementById('previewCount');
        elements.executionSection = document.getElementById('executionSection');
        elements.executionStatus = document.getElementById('executionStatus');
        elements.requestBodySection = document.getElementById('requestBodySection');
        elements.requestBodyDisplay = document.getElementById('requestBodyDisplay');
        elements.bySelect = document.getElementById('bySelect');
        elements.bySelectContainer = document.getElementById('bySelectContainer');
        elements.singleDateInput = document.getElementById('singleDateInput');
        elements.dateRangeInputs = document.getElementById('dateRangeInputs');

        elements.apiEndpoint = apiEndpoint || '/backload';

        initializePickers();
        setupEventListeners();

        if (window.FlowlordUtils && elements.previewTableBody) {
            window.FlowlordUtils.enableCellActions(elements.previewTableBody);
        }

        initializeDates();
        applyQueryParams();
    }

    function initializePickers() {
        if (window.FlowlordDateTimePicker) {
            pickers.at = window.FlowlordDateTimePicker.create('atPicker', {
                onChange: updatePreviewButton
            });
            pickers.from = window.FlowlordDateTimePicker.create('fromPicker', {
                onChange: updatePreviewButton
            });
            pickers.to = window.FlowlordDateTimePicker.create('toPicker', {
                onChange: updatePreviewButton
            });
        }
    }

    function getUniqueTasks(workflowFilterValue) {
        const taskSet = new Set();
        allPhases.forEach(p => {
            if (!workflowFilterValue || p.workflow === workflowFilterValue) {
                taskSet.add(p.task);
            }
        });
        return Array.from(taskSet).sort();
    }

    function setupEventListeners() {
        document.querySelectorAll('.toggle-btn').forEach(btn => {
            btn.addEventListener('click', handleDateModeToggle);
        });

        elements.taskSearch.addEventListener('input', handleTaskSearchInput);
        elements.taskSearch.addEventListener('focus', handleTaskSearchFocus);
        elements.taskSearch.addEventListener('keydown', handleTaskSearchKeydown);
        elements.taskDropdown.addEventListener('click', handleDropdownClick);
        document.addEventListener('click', handleDocumentClick);
        elements.workflowFilter.addEventListener('change', handleWorkflowFilterChange);
        elements.jobSelect.addEventListener('change', handleJobSelectChange);
        elements.previewBtn.addEventListener('click', handlePreviewClick);
        elements.executeBtn.addEventListener('click', handleExecuteClick);
        elements.resetBtn.addEventListener('click', handleResetClick);
    }

    function handleDateModeToggle() {
        document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');

        if (this.dataset.mode === 'range') {
            elements.dateRangeInputs.style.display = 'block';
            elements.singleDateInput.style.display = 'none';
            elements.bySelectContainer.style.display = 'block';
        } else {
            elements.dateRangeInputs.style.display = 'none';
            elements.singleDateInput.style.display = 'block';
            elements.bySelectContainer.style.display = 'none';
        }
        updatePreviewButton();
    }

    function handleTaskSearchInput() {
        clearTimeout(searchTimeout);
        const query = this.value.trim();

        searchTimeout = setTimeout(() => {
            showTaskDropdown(query);
        }, 100);
    }

    function handleTaskSearchFocus() {
        this.value = '';
        elements.taskSelect.value = '';
        selectedDropdownIndex = -1;
        showTaskDropdown('');
    }

    function handleTaskSearchKeydown(e) {
        const items = elements.taskDropdown.querySelectorAll('.search-dropdown-item');
        if (items.length === 0) return;

        switch (e.key) {
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
                    selectTask(items[selectedDropdownIndex].dataset.task);
                }
                break;
            case 'Escape':
                e.preventDefault();
                elements.taskDropdown.style.display = 'none';
                selectedDropdownIndex = -1;
                break;
        }
    }

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

    function selectTask(task) {
        elements.taskSearch.value = task;
        elements.taskSelect.value = task;
        elements.taskDropdown.style.display = 'none';
        selectedDropdownIndex = -1;
        onTaskSelected(task);
    }

    function showTaskDropdown(query) {
        const workflow = elements.workflowFilter.value;
        const tasks = getUniqueTasks(workflow);
        const matches = query
            ? tasks.filter(t => t.toLowerCase().includes(query.toLowerCase()))
            : tasks;

        selectedDropdownIndex = -1;

        if (matches.length > 0) {
            elements.taskDropdown.innerHTML = matches.map(task =>
                `<div class="search-dropdown-item" data-task="${window.FlowlordUtils.escapeHtml(task)}">${query ? highlightMatch(task, query) : window.FlowlordUtils.escapeHtml(task)}</div>`
            ).join('');
            elements.taskDropdown.style.display = 'block';
        } else {
            elements.taskDropdown.innerHTML = '<div class="search-dropdown-empty">No matching tasks</div>';
            elements.taskDropdown.style.display = 'block';
        }
    }

    function highlightMatch(text, query) {
        const idx = text.toLowerCase().indexOf(query.toLowerCase());
        if (idx === -1) return window.FlowlordUtils.escapeHtml(text);
        return window.FlowlordUtils.escapeHtml(text.slice(0, idx)) + '<strong>' + window.FlowlordUtils.escapeHtml(text.slice(idx, idx + query.length)) + '</strong>' + window.FlowlordUtils.escapeHtml(text.slice(idx + query.length));
    }

    function handleDropdownClick(e) {
        const item = e.target.closest('.search-dropdown-item');
        if (item) {
            selectTask(item.dataset.task);
        }
    }

    function handleDocumentClick(e) {
        if (!elements.taskSearch.contains(e.target) && !elements.taskDropdown.contains(e.target)) {
            elements.taskDropdown.style.display = 'none';
            selectedDropdownIndex = -1;
        }
    }

    function handleWorkflowFilterChange() {
        elements.taskSearch.value = '';
        elements.taskSelect.value = '';
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        setSelectedWorkflow('');
        hideTemplateInfo();
        updatePreviewButton();
    }

    function populateJobOptions(phases) {
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        const withJobs = phases.filter(p => p.job);
        if (withJobs.length === 0) {
            elements.jobSelect.disabled = true;
            return false;
        }

        const jobNames = [...new Set(withJobs.map(p => p.job))].sort();
        jobNames.forEach(job => {
            const jobPhases = withJobs.filter(p => p.job === job);
            const showWorkflowInLabel = jobPhases.length > 1;
            jobPhases.forEach(phase => {
                const option = document.createElement('option');
                option.value = job;
                option.dataset.workflow = phase.workflow || '';
                if (showWorkflowInLabel && phase.workflow) {
                    option.textContent = job + ' (' + phase.workflow + ')';
                } else {
                    option.textContent = job;
                }
                elements.jobSelect.appendChild(option);
            });
        });
        elements.jobSelect.disabled = false;
        return true;
    }

    function onTaskSelected(task) {
        const workflow = elements.workflowFilter.value;
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        hideTemplateInfo();
        setSelectedWorkflow('');

        const phases = allPhases.filter(p =>
            p.task === task && (!workflow || p.workflow === workflow)
        );

        if (populateJobOptions(phases)) {
            updatePreviewButton();
            return;
        }

        const phase = phases[0];
        if (phase) {
            setSelectedWorkflow(phase.workflow || '');
            showTemplateInfo(phase);
        }
        updatePreviewButton();
    }

    function handleJobSelectChange() {
        const task = elements.taskSelect.value;
        const job = this.value;
        const selectedOption = this.options[this.selectedIndex];
        const optionWorkflow = selectedOption && selectedOption.dataset ? selectedOption.dataset.workflow : '';
        const workflow = optionWorkflow || elements.workflowFilter.value;

        setSelectedWorkflow(workflow);

        const phase = findPhase(task, job, workflow);
        if (phase) {
            showTemplateInfo(phase);
        }
        updatePreviewButton();
    }

    function selectJobOption(job, workflow) {
        if (!job) return;
        for (let i = 0; i < elements.jobSelect.options.length; i++) {
            const opt = elements.jobSelect.options[i];
            if (opt.value !== job) continue;
            if (workflow && opt.dataset.workflow && opt.dataset.workflow !== workflow) continue;
            elements.jobSelect.selectedIndex = i;
            handleJobSelectChange.call(elements.jobSelect);
            return;
        }
    }

    function formatRule(str) {
        if (!str) return '(no rule)';
        return str.split('&').join('\n');
    }

    function showTemplateInfo(phase) {
        currentPhase = phase;
        setSelectedWorkflow(phase.workflow || selectedWorkflow);
        elements.templateSection.style.display = 'block';
        if (elements.workflowDisplay) {
            elements.workflowDisplay.textContent = phase.workflow || '(unknown workflow)';
        }
        elements.templateDisplay.textContent = phase.template || '(no template)';
        elements.ruleDisplay.textContent = formatRule(phase.rule);

        const metaRegex = /\{meta:(\w+)\}/g;
        const metaKeys = [];
        let match;
        while ((match = metaRegex.exec(phase.template)) !== null) {
            if (!metaKeys.includes(match[1])) {
                metaKeys.push(match[1]);
            }
        }

        const hasMetaFile = phase.rule && phase.rule.includes('meta-file=');

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

    function hideTemplateInfo() {
        currentPhase = null;
        setSelectedWorkflow('');
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

    function getDateMode() {
        const activeBtn = document.querySelector('.toggle-btn.active');
        return activeBtn ? activeBtn.dataset.mode : 'single';
    }

    function updatePreviewButton() {
        const task = elements.taskSelect.value;
        const dateMode = getDateMode();
        let hasDate = false;

        if (dateMode === 'range') {
            const fromValue = pickers.from ? pickers.from.getValue() : null;
            const toValue = pickers.to ? pickers.to.getValue() : null;
            hasDate = (fromValue && fromValue.date) || (toValue && toValue.date);
        } else {
            const atValue = pickers.at ? pickers.at.getValue() : null;
            hasDate = atValue && atValue.date;
        }

        elements.previewBtn.disabled = !task || !hasDate;
    }

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

        if (selectedWorkflow) {
            request.Workflow = selectedWorkflow;
        }

        const by = elements.bySelect.value;
        if (by && by !== 'day') {
            request.By = by;
        }

        if (dateMode === 'range') {
            if (pickers.from) {
                const fromValue = pickers.from.getValue();
                if (fromValue.date) {
                    request.From = fromValue.formatted;
                }
            }
            if (pickers.to) {
                const toValue = pickers.to.getValue();
                if (toValue.date) {
                    request.To = toValue.formatted;
                }
            }
        } else if (pickers.at) {
            const atValue = pickers.at.getValue();
            if (atValue.date) {
                request.At = atValue.formatted;
            }
        }

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

        if (elements.metaFileSection.style.display !== 'none') {
            const metaFile = elements.metaFileInput.value.trim();
            if (metaFile) {
                request['meta-file'] = metaFile;
            }
        }

        return request;
    }

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

    async function handlePreviewClick() {
        const request = buildRequest(false);
        elements.requestBodySection.style.display = 'block';
        elements.requestBodyDisplay.textContent = JSON.stringify(request, null, 2);

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

    function messageFromApiResponse(responseText, fallback) {
        if (responseText == null || responseText === '') {
            return fallback || 'Request failed';
        }
        try {
            const j = JSON.parse(responseText);
            if (j && typeof j.Status === 'string' && j.Status.length > 0) {
                return j.Status;
            }
        } catch (e) { /* use raw */ }
        return responseText;
    }

    async function handleExecuteClick() {
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
            let data = null;
            try {
                data = JSON.parse(responseText);
            } catch (e) {
                data = null;
            }

            if (!response.ok) {
                elements.executionSection.style.display = 'block';
                elements.executionStatus.className = 'execution-status error';
                elements.executionStatus.textContent = messageFromApiResponse(responseText, 'Execution failed');
                return;
            }

            if (!data) {
                elements.executionSection.style.display = 'block';
                elements.executionStatus.className = 'execution-status error';
                elements.executionStatus.textContent = responseText || 'Invalid JSON response';
                return;
            }

            showExecutionResults(data);
            elements.executionSection.style.display = 'none';
            elements.executeBtn.style.display = 'none';
        } catch (error) {
            elements.executionSection.style.display = 'block';
            elements.executionStatus.className = 'execution-status error';
            elements.executionStatus.textContent = error.message || String(error);
        } finally {
            setButtonLoading(elements.executeBtn, false, 'Execute Backload');
        }
    }

    function handleResetClick() {
        elements.taskSearch.value = '';
        elements.taskSelect.value = '';
        elements.workflowFilter.value = '';
        elements.jobSelect.innerHTML = '<option value="">Select a job...</option>';
        elements.jobSelect.disabled = true;
        elements.bySelect.value = 'day';
        setSelectedWorkflow('');

        if (pickers.at) pickers.at.setValue('', '');
        if (pickers.from) pickers.from.setValue('', '');
        if (pickers.to) pickers.to.setValue('', '');

        document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        document.querySelector('.toggle-btn[data-mode="single"]').classList.add('active');
        elements.singleDateInput.style.display = 'block';
        elements.dateRangeInputs.style.display = 'none';
        elements.bySelectContainer.style.display = 'none';

        hideTemplateInfo();
        initializeDates();
        if (elements.previewResultsHeading) {
            elements.previewResultsHeading.textContent = 'Preview Results';
        }
        updatePreviewButton();
    }

    function renderTasksIntoPreviewTable(tasks, emptyRowHtml, options) {
        options = options || {};
        const showIds = !!options.showIds;
        const colCount = showIds ? 6 : 5;

        elements.previewTableBody.innerHTML = '';
        if (tasks && tasks.length > 0) {
            tasks.forEach((task, index) => {
                const row = document.createElement('tr');
                let idCell = '';
                if (showIds && task.id) {
                    const date = taskCreatedDate(task);
                    const href = taskPageBase() + '?date=' + encodeURIComponent(date) + '&id=' + encodeURIComponent(task.id);
                    idCell = `<td class="id-cell id-column"><a class="task-id-link" href="${window.FlowlordUtils.escapeAttr(href)}">${window.FlowlordUtils.escapeHtml(task.id)}</a></td>`;
                } else if (showIds) {
                    idCell = '<td class="id-cell id-column"></td>';
                }
                row.innerHTML = `
                    <td class="num-cell num-column">${index + 1}</td>
                    ${idCell}
                    <td class="type-cell type-column copyable" title="Click for actions">${window.FlowlordUtils.escapeHtml(task.type || '')}</td>
                    <td class="job-cell job-column copyable" title="Click for actions">${window.FlowlordUtils.escapeHtml(task.job || '')}</td>
                    <td class="info-cell info-column expandable copyable" title="Click for actions">${window.FlowlordUtils.escapeHtml(task.info || '')}</td>
                    <td class="meta-cell meta-column expandable copyable" title="Click for actions">${window.FlowlordUtils.escapeHtml(task.meta || '')}</td>
                `;
                elements.previewTableBody.appendChild(row);
            });
            if (window.FlowlordUtils) {
                window.FlowlordUtils.enableCellActions(elements.previewTableBody);
            }
        } else {
            elements.previewTableBody.innerHTML = emptyRowHtml || `<tr><td colspan="${colCount}" class="no-tasks">No tasks</td></tr>`;
        }
    }

    function setPreviewIdColumnVisible(visible) {
        if (elements.previewIdHeader) {
            elements.previewIdHeader.style.display = visible ? '' : 'none';
        }
    }

    function showPreviewResults(data) {
        if (elements.previewResultsHeading) {
            elements.previewResultsHeading.textContent = 'Preview Results';
        }
        setPreviewIdColumnVisible(false);
        elements.previewSection.style.display = 'block';
        elements.previewStatus.className = 'preview-status info';
        elements.previewStatus.textContent = data.Status || 'Dry run complete';

        renderTasksIntoPreviewTable(data.Tasks, '<tr><td colspan="5" class="no-tasks">No tasks would be created</td></tr>');

        if (data.Tasks && data.Tasks.length > 0) {
            elements.previewCount.textContent = `Total tasks to be created: ${data.Count}`;
            elements.executeBtn.style.display = 'inline-block';
            elements.executeBtn.disabled = false;
        } else {
            elements.previewCount.textContent = '';
            elements.executeBtn.style.display = 'none';
        }
    }

    function showExecutionResults(data) {
        if (elements.previewResultsHeading) {
            elements.previewResultsHeading.textContent = 'Execution results';
        }
        setPreviewIdColumnVisible(true);
        elements.previewSection.style.display = 'block';
        elements.previewStatus.className = 'preview-status execution-success';
        elements.previewStatus.innerHTML =
            '<strong>Executed (not a dry run)</strong><br>' +
            window.FlowlordUtils.escapeHtml(data.Status || '');

        renderTasksIntoPreviewTable(
            data.Tasks,
            '<tr><td colspan="6" class="no-tasks">No tasks were sent</td></tr>',
            { showIds: true }
        );

        const n = typeof data.Count === 'number' ? data.Count : (data.Tasks && data.Tasks.length) || 0;
        const viewAllHref = buildViewAllTasksHref(data.Tasks);
        let countHtml = `Created ${n} task(s). Jobs were sent to the task bus.`;
        if (viewAllHref && n > 0) {
            countHtml += ` <a class="task-id-link" href="${window.FlowlordUtils.escapeAttr(viewAllHref)}">View all ${n} created tasks</a>`;
        }
        elements.previewCount.innerHTML = countHtml;
    }

    function initializeDates() {
        const today = new Date().toISOString().split('T')[0];
        if (pickers.at) pickers.at.setValue(today, '');
        if (pickers.from) pickers.from.setValue(today, '');
        if (pickers.to) pickers.to.setValue(today, '');
        updatePreviewButton();
    }

    function setMetaFieldValue(key, value) {
        const input = document.querySelector('.meta-input[data-meta-key="' + key + '"]');
        if (input) {
            input.value = value;
        }
    }

    function applyQueryParams() {
        const params = new URLSearchParams(window.location.search);
        const task = params.get('task');
        if (!task) return;

        const workflow = params.get('workflow') || '';
        const job = params.get('job') || '';
        const at = params.get('at') || '';

        if (workflow) {
            elements.workflowFilter.value = workflow;
        }

        selectTask(task);

        if (job) {
            selectJobOption(job, workflow);
        } else if (workflow) {
            const phase = findPhase(task, '', workflow);
            if (phase) {
                setSelectedWorkflow(workflow);
                showTemplateInfo(phase);
            }
        }

        if (at && pickers.at) {
            document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
            const singleBtn = document.querySelector('.toggle-btn[data-mode="single"]');
            if (singleBtn) singleBtn.classList.add('active');
            elements.dateRangeInputs.style.display = 'none';
            elements.singleDateInput.style.display = 'block';
            elements.bySelectContainer.style.display = 'none';
            pickers.at.setValue(at, '');
        }

        params.forEach((value, key) => {
            if (key.startsWith('meta.')) {
                setMetaFieldValue(key.slice(5), value);
            }
        });

        updatePreviewButton();

        if (params.get('preview') === '1') {
            setTimeout(function() {
                if (!elements.previewBtn.disabled) {
                    handlePreviewClick();
                }
            }, 0);
        }
    }

    function buildBackloadUrl(taskRow) {
        if (window.FlowlordUtils && window.FlowlordUtils.buildBackloadUrl) {
            return window.FlowlordUtils.buildBackloadUrl(taskRow);
        }
        return '/web/backload';
    }

    window.FlowlordBackload = {
        init: init,
        buildBackloadUrl: buildBackloadUrl
    };
})();

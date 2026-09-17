// About page task import
(function() {
    'use strict';

    const elements = {};

    function init(apiEndpoint) {
        elements.fileInput = document.getElementById('importFile');
        elements.importBtn = document.getElementById('importBtn');
        elements.status = document.getElementById('importStatus');
        elements.errorsSection = document.getElementById('importErrors');
        elements.errorsList = document.getElementById('importErrorsList');

        if (!elements.fileInput || !elements.importBtn) {
            return;
        }

        elements.fileInput.addEventListener('change', function() {
            elements.importBtn.disabled = !elements.fileInput.files.length;
            hideResults();
        });

        elements.importBtn.addEventListener('click', function() {
            uploadFile(apiEndpoint);
        });
    }

    function hideResults() {
        elements.status.style.display = 'none';
        elements.errorsSection.style.display = 'none';
        elements.errorsList.innerHTML = '';
    }

    function showStatus(message, className) {
        elements.status.textContent = message;
        elements.status.className = 'preview-status ' + (className || 'info');
        elements.status.style.display = 'block';
    }

    function showErrors(errors) {
        if (!errors || errors.length === 0) {
            elements.errorsSection.style.display = 'none';
            return;
        }
        elements.errorsList.innerHTML = '';
        errors.forEach(function(err) {
            const li = document.createElement('li');
            li.textContent = err;
            elements.errorsList.appendChild(li);
        });
        elements.errorsSection.style.display = 'block';
    }

    function uploadFile(apiEndpoint) {
        const file = elements.fileInput.files[0];
        if (!file) {
            return;
        }

        elements.importBtn.disabled = true;
        showStatus('Importing…', 'info');

        const formData = new FormData();
        formData.append('file', file);

        fetch(apiEndpoint, {
            method: 'POST',
            body: formData
        })
            .then(function(response) {
                if (!response.ok) {
                    return response.text().then(function(text) {
                        throw new Error(text || ('HTTP ' + response.status));
                    });
                }
                return response.json();
            })
            .then(function(result) {
                const summary = result.imported + ' imported, ' +
                    result.skipped + ' skipped, ' +
                    result.lines + ' lines parsed';
                let className = 'info';
                if (result.status === 'partial') {
                    className = 'execution-success';
                } else if (result.status === 'failed' || result.status.indexOf('sync failed') === 0) {
                    className = 'error';
                } else if (result.imported > 0) {
                    className = 'execution-success';
                }
                let message = summary;
                if (result.status && result.status !== 'ok') {
                    message += ' (' + result.status + ')';
                }
                if (result.imported > 0) {
                    message += '. Refresh the page to update table statistics.';
                }
                showStatus(message, className);
                showErrors(result.errors);
            })
            .catch(function(err) {
                showStatus('Import failed: ' + err.message, 'error');
            })
            .finally(function() {
                elements.importBtn.disabled = !elements.fileInput.files.length;
            });
    }

    window.FlowlordAbout = {
        init: init
    };
})();

// About page task import
(function() {
    'use strict';

    const elements = {};
    let pendingUpload = null;

    function formatFileSize(bytes) {
        if (bytes < 1024) {
            return bytes + ' B';
        }
        if (bytes < 1024 * 1024) {
            return (bytes / 1024).toFixed(1) + ' KB';
        }
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    function hasServerPath() {
        return elements.pathInput && elements.pathInput.value.trim().length > 0;
    }

    function remotePathError(path) {
        if (!/^(s3|gs|gcs|mc|mcs|minio):\/\//i.test(path)) {
            return 'Remote import paths must start with s3://, gs://, gcs://, mc://, or mcs://.';
        }
        if (path.indexOf('\\') >= 0) {
            return 'Remote paths cannot contain backslashes.';
        }
        const objectPart = path.replace(/^[^:]+:\/\/[^/]+\//, '');
        const segments = objectPart.split('/');
        for (let i = 0; i < segments.length; i++) {
            if (segments[i] === '.' || segments[i] === '..') {
                return 'Remote paths cannot include . or .. path segments.';
            }
        }
        return '';
    }

    function hasUpload() {
        return pendingUpload !== null;
    }

    function updateImportEnabled() {
        elements.importBtn.disabled = !(hasUpload() || hasServerPath());
    }

    function syncControls() {
        if (!elements.pathInput) {
            return;
        }
        elements.pathInput.disabled = hasUpload();
        if (elements.browseBtn) {
            elements.browseBtn.disabled = false;
        }
        updateImportEnabled();
    }

    function hideAttachment() {
        if (elements.attachmentRow) {
            elements.attachmentRow.hidden = true;
        }
    }

    function showAttachment(file) {
        if (!file || !elements.attachmentRow) {
            hideAttachment();
            return;
        }
        if (elements.attachmentName) {
            elements.attachmentName.textContent = file.name;
        }
        if (elements.attachmentSize) {
            elements.attachmentSize.textContent = formatFileSize(file.size);
        }
        elements.attachmentRow.hidden = false;
    }

    function resetImportSource() {
        pendingUpload = null;
        if (elements.fileInput) {
            elements.fileInput.value = '';
        }
        if (elements.pathInput) {
            elements.pathInput.value = '';
        }
        hideAttachment();
        syncControls();
    }

    function clearPendingUpload() {
        pendingUpload = null;
        if (elements.fileInput) {
            elements.fileInput.value = '';
        }
        hideAttachment();
        syncControls();
    }

    function init(apiEndpoint) {
        elements.pathInput = document.getElementById('importPath');
        elements.pathRow = document.getElementById('importPathRow');
        elements.fileInput = document.getElementById('importFile');
        elements.browseBtn = document.getElementById('importBrowseBtn');
        elements.attachmentRow = document.getElementById('importAttachmentRow');
        elements.attachmentName = document.getElementById('importAttachmentName');
        elements.attachmentSize = document.getElementById('importAttachmentSize');
        elements.attachmentClear = document.getElementById('importAttachmentClear');
        elements.importBtn = document.getElementById('importBtn');
        elements.status = document.getElementById('importStatus');
        elements.errorsSection = document.getElementById('importErrors');
        elements.errorsList = document.getElementById('importErrorsList');

        if (!elements.importBtn || !elements.pathInput) {
            return;
        }

        hideAttachment();
        syncControls();

        elements.pathInput.addEventListener('input', function() {
            if (hasUpload()) {
                return;
            }
            syncControls();
            hideResults();
        });

        if (elements.browseBtn && elements.fileInput) {
            elements.browseBtn.addEventListener('click', function() {
                elements.fileInput.click();
            });

            elements.fileInput.addEventListener('change', function() {
                const file = elements.fileInput.files[0];
                if (!file) {
                    return;
                }
                pendingUpload = file;
                elements.pathInput.value = '';
                showAttachment(file);
                syncControls();
                hideResults();
            });
        }

        if (elements.attachmentClear) {
            elements.attachmentClear.addEventListener('click', function() {
                clearPendingUpload();
                hideResults();
            });
        }

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
        if (!hasUpload() && !hasServerPath()) {
            return;
        }

        const wasUpload = hasUpload();

        const formData = new FormData();
        if (wasUpload) {
            formData.append('file', pendingUpload);
        } else {
            const remotePath = elements.pathInput.value.trim();
            const pathErr = remotePathError(remotePath);
            if (pathErr) {
                showStatus(pathErr, 'error');
                return;
            }
            formData.append('path', remotePath);
        }

        elements.importBtn.disabled = true;
        showStatus('Importing…', 'info');

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

                if (wasUpload && result.status !== 'failed') {
                    resetImportSource();
                } else if (!wasUpload && result.status !== 'failed') {
                    if (elements.pathInput) {
                        elements.pathInput.value = '';
                    }
                    syncControls();
                }
            })
            .catch(function(err) {
                showStatus('Import failed: ' + err.message, 'error');
            })
            .finally(function() {
                syncControls();
            });
    }

    window.FlowlordAbout = {
        init: init
    };
})();
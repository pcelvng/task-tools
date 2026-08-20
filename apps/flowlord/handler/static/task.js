// Task page functionality — sorting and DataGrip-style multi-select column filters
(function() {
    'use strict';

    var RESULT_OPTIONS = [
        { value: 'complete', label: 'Completed' },
        { value: 'error', label: 'Errors' },
        { value: 'alert', label: 'Alerts' },
        { value: 'warn', label: 'Warnings' },
        { value: 'running', label: 'Running' }
    ];

    function initTaskPage(config) {
        config = config || {};
        var table = document.getElementById('taskTable');
        if (!table) {
            return;
        }

        var tbody = table.querySelector('tbody');
        var taskTypes = config.taskTypes || [];
        var jobMap = new Map(config.jobsByType || []);
        var filters = {
            id: toArray(config.filters && config.filters.id),
            type: toArray(config.filters && config.filters.type),
            job: toArray(config.filters && config.filters.job),
            result: toArray(config.filters && config.filters.result)
        };
        var currentSort = { column: null, direction: 'asc' };
        var activePopup = null;
        var dismissPopup = null;

        function toArray(v) {
            if (!v) return [];
            if (Array.isArray(v)) return v.filter(Boolean);
            return String(v).split(',').map(function(s) { return s.trim(); }).filter(Boolean);
        }

        function parseListParam(urlParams, name) {
            var all = urlParams.getAll(name);
            var out = [];
            all.forEach(function(v) {
                String(v).split(',').forEach(function(p) {
                    p = p.trim();
                    if (p) out.push(p);
                });
            });
            return out;
        }

        function getUrlParams() {
            var urlParams = new URLSearchParams(window.location.search);
            return {
                date: urlParams.get('date') || '',
                id: parseListParam(urlParams, 'id'),
                type: parseListParam(urlParams, 'type'),
                job: parseListParam(urlParams, 'job'),
                result: parseListParam(urlParams, 'result'),
                sort: urlParams.get('sort') || '',
                direction: urlParams.get('direction') || 'asc'
            };
        }

        function saveScroll() {
            try {
                sessionStorage.setItem('flowlordTaskScrollY', String(window.scrollY));
            } catch (e) { /* ignore */ }
        }

        function restoreScroll() {
            var y = null;
            try {
                y = sessionStorage.getItem('flowlordTaskScrollY');
                sessionStorage.removeItem('flowlordTaskScrollY');
            } catch (e) {
                return;
            }
            if (y === null) return;
            var top = parseInt(y, 10);
            if (!isNaN(top)) {
                window.scrollTo(0, top);
            }
        }

        function setListParam(url, key, values) {
            url.searchParams.delete(key);
            values = toArray(values);
            if (values.length === 0) return;
            // Comma-separated — uri package accepts both this and repeated params
            url.searchParams.set(key, values.join(','));
        }

        function navigate(updates) {
            var params = getUrlParams();
            var next = Object.assign({}, params, updates || {});
            var url = new URL(window.location);

            if (next.date) url.searchParams.set('date', next.date);
            else url.searchParams.delete('date');

            setListParam(url, 'id', next.id);
            setListParam(url, 'type', next.type);
            setListParam(url, 'job', next.job);
            setListParam(url, 'result', next.result);

            if (next.sort) {
                url.searchParams.set('sort', next.sort);
                url.searchParams.set('direction', next.direction || 'asc');
            } else {
                url.searchParams.delete('sort');
                url.searchParams.delete('direction');
            }

            url.searchParams.delete('page');
            saveScroll();
            window.location.href = url.toString();
        }

        function applyFilter(column, value) {
            var params = getUrlParams();
            var updates = {
                date: params.date,
                id: params.id.slice(),
                type: params.type.slice(),
                job: params.job.slice(),
                result: params.result.slice(),
                sort: params.sort,
                direction: params.direction
            };

            if (column === 'id') {
                updates.id = toArray(value);
            } else if (column === 'type') {
                updates.type = toArray(value);
                // Drop jobs that are no longer valid for the selected types
                updates.job = pruneJobs(updates.job, updates.type);
            } else if (column === 'job') {
                updates.job = toArray(value);
            } else if (column === 'result') {
                updates.result = toArray(value);
            }

            navigate(updates);
        }

        function pruneJobs(jobs, types) {
            if (!types.length) return jobs;
            var allowed = new Set();
            types.forEach(function(t) {
                (jobMap.get(t) || []).forEach(function(j) { allowed.add(j); });
            });
            return jobs.filter(function(j) { return allowed.has(j); });
        }

        function jobsForTypes(types) {
            if (!types.length) {
                // No type filter: union of all jobs
                var all = new Set();
                jobMap.forEach(function(jobs) {
                    jobs.forEach(function(j) { all.add(j); });
                });
                return Array.from(all).sort();
            }
            var set = new Set();
            types.forEach(function(t) {
                (jobMap.get(t) || []).forEach(function(j) { set.add(j); });
            });
            return Array.from(set).sort();
        }

        function clearFilter(column, value) {
            var params = getUrlParams();
            var updates = {
                date: params.date,
                id: params.id.slice(),
                type: params.type.slice(),
                job: params.job.slice(),
                result: params.result.slice(),
                sort: params.sort,
                direction: params.direction
            };

            if (column === 'all') {
                updates.id = [];
                updates.type = [];
                updates.job = [];
                updates.result = [];
                updates.sort = '';
                updates.direction = 'asc';
            } else if (column === 'sort') {
                updates.sort = '';
                updates.direction = 'asc';
            } else if (value) {
                // Remove a single value from a multi-select column
                updates[column] = (updates[column] || []).filter(function(v) { return v !== value; });
                if (column === 'type') {
                    updates.job = pruneJobs(updates.job, updates.type);
                }
            } else {
                updates[column] = [];
                if (column === 'type') {
                    updates.job = [];
                }
            }

            navigate(updates);
        }

        function closePopup() {
            if (dismissPopup) {
                dismissPopup();
                dismissPopup = null;
            }
            if (activePopup) {
                activePopup.remove();
                activePopup = null;
            }
        }

        function positionPopup(popup, anchor) {
            var rect = anchor.getBoundingClientRect();
            popup.style.left = Math.min(rect.left, window.innerWidth - popup.offsetWidth - 8) + 'px';
            popup.style.top = (rect.bottom + 4) + 'px';
            if (rect.bottom + popup.offsetHeight + 8 > window.innerHeight && rect.top > popup.offsetHeight) {
                popup.style.top = (rect.top - popup.offsetHeight - 4) + 'px';
            }
        }

        function selectedSet(arr) {
            var s = new Set(toArray(arr));
            return s;
        }

        function openPopup(column, anchor) {
            closePopup();

            var popup = document.createElement('div');
            popup.className = 'column-filter-popup';
            popup.setAttribute('role', 'dialog');
            popup.setAttribute('aria-label', 'Filter ' + column);

            if (column === 'id') {
                popup.classList.add('column-filter-popup-tokens');
                popup.innerHTML =
                    '<div class="column-filter-title">Filter ID</div>' +
                    '<div class="token-input" tabindex="-1">' +
                    '<ul class="token-list" aria-label="Selected IDs"></ul>' +
                    '<input type="text" class="token-input-field" placeholder="Add ID, Enter or comma" autocomplete="off" spellcheck="false">' +
                    '</div>' +
                    '<div class="column-filter-hint">Paste multiple IDs separated by commas or newlines</div>' +
                    '<div class="column-filter-actions">' +
                    '<button type="button" class="btn btn-secondary btn-sm column-filter-clear">Clear</button>' +
                    '<button type="button" class="btn btn-primary btn-sm column-filter-apply">Apply</button>' +
                    '</div>';

                var tokens = toArray(filters.id).slice();
                var tokenList = popup.querySelector('.token-list');
                var tokenField = popup.querySelector('.token-input-field');
                var tokenWrap = popup.querySelector('.token-input');

                function renderTokens() {
                    tokenList.innerHTML = '';
                    tokens.forEach(function(id, idx) {
                        var li = document.createElement('li');
                        li.className = 'token';
                        li.innerHTML =
                            '<span class="token-text">' + escapeHtml(id) + '</span>' +
                            '<button type="button" class="token-remove" aria-label="Remove ' + escapeAttr(id) + '" data-index="' + idx + '">&times;</button>';
                        tokenList.appendChild(li);
                    });
                }

                function addTokensFromText(text) {
                    String(text || '').split(/[\s,]+/).forEach(function(part) {
                        part = part.trim();
                        if (part && tokens.indexOf(part) === -1) {
                            tokens.push(part);
                        }
                    });
                    renderTokens();
                }

                function commitPendingInput() {
                    var v = tokenField.value.trim();
                    if (!v) return;
                    addTokensFromText(v);
                    tokenField.value = '';
                }

                renderTokens();

                tokenList.addEventListener('click', function(e) {
                    var btn = e.target.closest('.token-remove');
                    if (!btn) return;
                    var idx = parseInt(btn.getAttribute('data-index'), 10);
                    if (!isNaN(idx)) {
                        tokens.splice(idx, 1);
                        renderTokens();
                        tokenField.focus();
                    }
                });

                tokenWrap.addEventListener('click', function(e) {
                    if (e.target === tokenWrap || e.target === tokenList) {
                        tokenField.focus();
                    }
                });

                tokenField.addEventListener('keydown', function(e) {
                    if (e.key === 'Enter') {
                        e.preventDefault();
                        if (tokenField.value.trim()) {
                            commitPendingInput();
                        } else {
                            applyFilter('id', tokens);
                        }
                    } else if (e.key === ',') {
                        e.preventDefault();
                        commitPendingInput();
                    } else if (e.key === 'Backspace' && !tokenField.value && tokens.length) {
                        tokens.pop();
                        renderTokens();
                    }
                });

                tokenField.addEventListener('input', function() {
                    // Commit when user types a trailing comma/space separator
                    var v = tokenField.value;
                    if (/[\s,]/.test(v) && /[^\s,]/.test(v)) {
                        addTokensFromText(v);
                        tokenField.value = '';
                    }
                });

                tokenField.addEventListener('paste', function(e) {
                    var text = (e.clipboardData || window.clipboardData).getData('text');
                    if (!text) return;
                    e.preventDefault();
                    addTokensFromText(text);
                    tokenField.value = '';
                });

                popup.querySelector('.column-filter-apply').addEventListener('click', function() {
                    commitPendingInput();
                    applyFilter('id', tokens);
                });
                popup.querySelector('.column-filter-clear').addEventListener('click', function() {
                    clearFilter('id');
                });
            } else {
                var title = column === 'type' ? 'Filter Type' : (column === 'job' ? 'Filter Job' : 'Filter Result');
                var options = [];
                var current = selectedSet(filters[column]);

                if (column === 'type') {
                    options = taskTypes.map(function(t) { return { value: t, label: t }; });
                } else if (column === 'job') {
                    options = jobsForTypes(filters.type).map(function(j) {
                        return { value: j, label: j };
                    });
                } else {
                    options = RESULT_OPTIONS.slice();
                }

                var html = '<div class="column-filter-title">' + title + '</div>';
                if (options.length === 0) {
                    html += '<div class="column-filter-empty">No options available</div>';
                } else {
                    html += '<input type="text" class="column-filter-search" placeholder="Search...">';
                    html += '<ul class="column-filter-list">';
                    options.forEach(function(opt) {
                        var checked = current.has(opt.value) ? ' checked' : '';
                        html += '<li class="column-filter-option">' +
                            '<label class="column-filter-check">' +
                            '<input type="checkbox" value="' + escapeAttr(opt.value) + '"' + checked + '>' +
                            '<span>' + escapeHtml(opt.label) + '</span>' +
                            '</label></li>';
                    });
                    html += '</ul>';
                }
                html += '<div class="column-filter-actions">' +
                    '<button type="button" class="btn btn-secondary btn-sm column-filter-clear">Clear</button>' +
                    '<button type="button" class="btn btn-primary btn-sm column-filter-apply">Apply</button>' +
                    '</div>';
                popup.innerHTML = html;

                popup.querySelector('.column-filter-clear').addEventListener('click', function() {
                    clearFilter(column);
                });
                var applyBtn = popup.querySelector('.column-filter-apply');
                if (applyBtn) {
                    applyBtn.addEventListener('click', function() {
                        var selected = [];
                        popup.querySelectorAll('.column-filter-list input[type="checkbox"]:checked').forEach(function(cb) {
                            selected.push(cb.value);
                        });
                        applyFilter(column, selected);
                    });
                }

                var list = popup.querySelector('.column-filter-list');
                var search = popup.querySelector('.column-filter-search');
                if (search && list) {
                    search.addEventListener('input', function() {
                        var q = search.value.toLowerCase();
                        list.querySelectorAll('.column-filter-option').forEach(function(li) {
                            var text = li.textContent.toLowerCase();
                            li.style.display = text.indexOf(q) !== -1 ? '' : 'none';
                        });
                    });
                }
            }

            document.body.appendChild(popup);
            activePopup = popup;
            positionPopup(popup, anchor);

            // Keep interactions inside the popup from dismissing it
            popup.addEventListener('mousedown', function(e) {
                e.stopPropagation();
            });

            var focusEl = popup.querySelector('.token-input-field, .column-filter-input, .column-filter-search');
            if (focusEl) {
                // Defer focus so layout/scroll from opening does not race dismiss handlers
                setTimeout(function() {
                    if (activePopup !== popup) return;
                    focusEl.focus();
                }, 0);
            }

            function onDocMouseDown(e) {
                if (popup.contains(e.target) || anchor.contains(e.target)) return;
                closePopup();
            }
            function onKey(e) {
                if (e.key === 'Escape') closePopup();
            }

            dismissPopup = function() {
                document.removeEventListener('mousedown', onDocMouseDown);
                document.removeEventListener('keydown', onKey);
            };

            setTimeout(function() {
                document.addEventListener('mousedown', onDocMouseDown);
            }, 0);
            document.addEventListener('keydown', onKey);
        }

        function escapeHtml(text) {
            if (window.FlowlordUtils && window.FlowlordUtils.escapeHtml) {
                return window.FlowlordUtils.escapeHtml(text);
            }
            var div = document.createElement('div');
            div.textContent = String(text);
            return div.innerHTML;
        }

        function escapeAttr(text) {
            return String(text)
                .replace(/&/g, '&amp;')
                .replace(/"/g, '&quot;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
        }

        var params = getUrlParams();
        if (params.sort) {
            currentSort = { column: params.sort, direction: params.direction };
        }

        table.querySelectorAll('th.sortable .th-sort').forEach(function(btn) {
            btn.title = 'Sort: click ascending, again descending, again to clear';
            btn.addEventListener('click', function(e) {
                e.stopPropagation();
                closePopup();
                var th = btn.closest('th.sortable');
                if (!th) return;
                var column = th.dataset.sort;
                var p = getUrlParams();
                var sort = column;
                var direction = 'asc';

                // Cycle: none → asc → desc → none (clear)
                if (currentSort.column === column) {
                    if (currentSort.direction === 'asc') {
                        direction = 'desc';
                    } else {
                        sort = '';
                        direction = 'asc';
                    }
                }

                currentSort = sort ? { column: sort, direction: direction } : { column: null, direction: 'asc' };
                navigate({
                    date: p.date,
                    id: p.id,
                    type: p.type,
                    job: p.job,
                    result: p.result,
                    sort: sort,
                    direction: direction
                });
            });
        });

        table.querySelectorAll('th.filterable .th-filter').forEach(function(btn) {
            btn.addEventListener('click', function(e) {
                e.stopPropagation();
                var th = btn.closest('th.filterable');
                if (!th) return;
                openPopup(th.dataset.filter, btn);
            });
        });

        var summary = document.getElementById('filterSummary');
        if (summary) {
            summary.addEventListener('click', function(e) {
                var clearBtn = e.target.closest('.filter-chip-clear');
                if (clearBtn) {
                    e.preventDefault();
                    var chip = clearBtn.closest('.filter-chip');
                    if (chip) {
                        clearFilter(chip.getAttribute('data-clear'), chip.getAttribute('data-value') || '');
                    }
                    return;
                }
                if (e.target.closest('#clearAllFilters')) {
                    clearFilter('all');
                }
            });
        }

        if (tbody) {
            tbody.addEventListener('click', function(e) {
                var cell = e.target.closest('.expandable');
                if (cell) {
                    e.stopPropagation();
                    cell.classList.toggle('expanded');
                }
            });
            if (window.FlowlordUtils) {
                window.FlowlordUtils.enableCopyableCells(tbody);
            }
        }

        restoreScroll();
    }

    window.clearFilters = function() {
        var url = new URL(window.location);
        url.searchParams.delete('id');
        url.searchParams.delete('type');
        url.searchParams.delete('job');
        url.searchParams.delete('result');
        url.searchParams.delete('sort');
        url.searchParams.delete('direction');
        url.searchParams.delete('page');
        window.location.href = url.toString();
    };

    window.toggleCollapsible = function(sectionId) {
        var content = document.getElementById(sectionId + '-content');
        var toggle = document.getElementById(sectionId + '-toggle');

        if (content.classList.contains('collapsed')) {
            content.classList.remove('collapsed');
            toggle.classList.add('expanded');
        } else {
            content.classList.add('collapsed');
            toggle.classList.remove('expanded');
        }
    };

    window.FlowlordTask = {
        init: initTaskPage
    };
})();

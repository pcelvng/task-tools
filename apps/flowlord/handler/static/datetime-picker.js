// Reusable DateTime Picker Component
// Creates a custom date+hour picker with calendar and 24-hour grid side by side
(function() {
    'use strict';

    /**
     * Create a datetime picker instance
     * @param {string} inputId - ID of the text input element
     * @param {Object} options - Configuration options
     * @param {Function} options.onChange - Callback when value changes
     * @returns {Object} Picker instance with open(), close(), getValue(), setValue() methods
     */
    function create(inputId, options) {
        options = options || {};
        
        const input = document.getElementById(inputId);
        if (!input) {
            console.error('DateTimePicker: Input not found:', inputId);
            return null;
        }

        // State
        let state = {
            currentMonth: new Date(),
            selectedDate: '',
            selectedHour: '',
            isOpen: false
        };

        // Create button and dropdown elements
        const container = input.parentElement;
        container.classList.add('datetime-picker-container');

        // Create calendar button
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'datetime-picker-btn';
        btn.title = 'Open calendar';
        btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M3.5 0a.5.5 0 0 1 .5.5V1h8V.5a.5.5 0 0 1 1 0V1h1a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V3a2 2 0 0 1 2-2h1V.5a.5.5 0 0 1 .5-.5zM1 4v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V4H1z"/></svg>';
        container.appendChild(btn);

        // Create dropdown
        const dropdown = document.createElement('div');
        dropdown.className = 'datetime-dropdown';
        dropdown.style.display = 'none';
        container.appendChild(dropdown);

        // Parse input value
        function parseInputValue() {
            const value = input.value.trim();
            if (!value) {
                state.selectedDate = '';
                state.selectedHour = '';
                return;
            }

            if (value.includes('T')) {
                const parts = value.split('T');
                state.selectedDate = parts[0];
                state.selectedHour = parts[1].substring(0, 2);
            } else if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
                state.selectedDate = value;
                state.selectedHour = '';
            }
        }

        // Update input display
        function updateInputDisplay() {
            let displayValue = state.selectedDate;
            if (state.selectedHour) {
                displayValue += 'T' + state.selectedHour;
            }
            input.value = displayValue;
        }

        // Render dropdown content
        function render() {
            const year = state.currentMonth.getFullYear();
            const month = state.currentMonth.getMonth();
            const today = new Date().toISOString().split('T')[0];

            let html = '<div class="datetime-dropdown-content">';
            
            // Left side: Calendar
            html += '<div class="datetime-calendar-section">';
            
            // Calendar header
            html += '<div class="datetime-calendar-header">';
            html += '<button type="button" class="datetime-month-nav" data-dir="-1">◀</button>';
            html += '<span class="datetime-month-label">' + state.currentMonth.toLocaleDateString('en-US', {month: 'long', year: 'numeric'}) + '</span>';
            html += '<button type="button" class="datetime-month-nav" data-dir="1">▶</button>';
            html += '</div>';

            // Calendar grid
            html += '<div class="datetime-calendar-grid">';
            html += '<div class="datetime-day-header">Su</div><div class="datetime-day-header">Mo</div><div class="datetime-day-header">Tu</div>';
            html += '<div class="datetime-day-header">We</div><div class="datetime-day-header">Th</div><div class="datetime-day-header">Fr</div><div class="datetime-day-header">Sa</div>';

            const firstDay = new Date(year, month, 1);
            const lastDay = new Date(year, month + 1, 0);
            const startDay = firstDay.getDay();

            // Empty cells
            for (let i = 0; i < startDay; i++) {
                html += '<div class="datetime-day-cell empty"></div>';
            }

            // Days
            for (let day = 1; day <= lastDay.getDate(); day++) {
                const dateStr = year + '-' + String(month + 1).padStart(2, '0') + '-' + String(day).padStart(2, '0');
                let classes = 'datetime-day-cell';
                if (dateStr === state.selectedDate) classes += ' selected';
                if (dateStr === today) classes += ' today';
                html += '<div class="' + classes + '" data-date="' + dateStr + '">' + day + '</div>';
            }
            html += '</div>';

            // Today button
            html += '<div class="datetime-calendar-footer">';
            html += '<button type="button" class="btn btn-sm datetime-today-btn">Today</button>';
            html += '</div>';
            
            html += '</div>'; // end calendar section

            // Right side: Hour grid
            html += '<div class="datetime-hour-section">';
            html += '<div class="datetime-hour-label">Hour</div>';
            html += '<div class="datetime-hour-grid">';

            // Hours 00-23 in 4 columns
            for (let h = 0; h < 24; h++) {
                const hourStr = String(h).padStart(2, '0');
                const isSelected = state.selectedHour === hourStr;
                html += '<div class="datetime-hour-cell' + (isSelected ? ' selected' : '') + '" data-hour="' + hourStr + '">' + hourStr + '</div>';
            }

            html += '</div>';
            
            // No hour option
            html += '<div class="datetime-hour-cell no-hour' + (state.selectedHour === '' ? ' selected' : '') + '" data-hour="">No hour</div>';
            
            html += '</div>'; // end hour section

            html += '</div>'; // end content

            dropdown.innerHTML = html;
            attachDropdownListeners();
        }

        // Attach event listeners to dropdown elements
        function attachDropdownListeners() {
            // Month navigation
            dropdown.querySelectorAll('.datetime-month-nav').forEach(function(navBtn) {
                navBtn.addEventListener('click', function(e) {
                    e.stopPropagation();
                    const dir = parseInt(this.dataset.dir);
                    state.currentMonth.setMonth(state.currentMonth.getMonth() + dir);
                    render();
                });
            });

            // Day selection
            dropdown.querySelectorAll('.datetime-day-cell:not(.empty)').forEach(function(cell) {
                cell.addEventListener('click', function(e) {
                    e.stopPropagation();
                    state.selectedDate = this.dataset.date;
                    updateInputDisplay();
                    render();
                    triggerChange();
                });
            });

            // Hour selection
            dropdown.querySelectorAll('.datetime-hour-cell').forEach(function(cell) {
                cell.addEventListener('click', function(e) {
                    e.stopPropagation();
                    state.selectedHour = this.dataset.hour;
                    updateInputDisplay();
                    render();
                    triggerChange();
                });
            });

            // Today button
            const todayBtn = dropdown.querySelector('.datetime-today-btn');
            if (todayBtn) {
                todayBtn.addEventListener('click', function(e) {
                    e.stopPropagation();
                    const today = new Date();
                    state.selectedDate = today.toISOString().split('T')[0];
                    state.currentMonth = today;
                    updateInputDisplay();
                    render();
                    triggerChange();
                });
            }
        }

        // Trigger onChange callback
        function triggerChange() {
            if (options.onChange) {
                options.onChange(getValue());
            }
        }

        // Open dropdown
        function open() {
            if (state.isOpen) return;
            
            parseInputValue();
            if (state.selectedDate) {
                state.currentMonth = new Date(state.selectedDate + 'T12:00:00');
            } else {
                state.currentMonth = new Date();
            }
            
            render();
            dropdown.style.display = 'block';
            state.isOpen = true;
        }

        // Close dropdown
        function close() {
            dropdown.style.display = 'none';
            state.isOpen = false;
        }

        // Toggle dropdown
        function toggle() {
            if (state.isOpen) {
                close();
            } else {
                open();
            }
        }

        // Get current value
        function getValue() {
            return {
                date: state.selectedDate,
                hour: state.selectedHour,
                formatted: state.selectedDate ? (state.selectedDate + (state.selectedHour ? 'T' + state.selectedHour : '')) : ''
            };
        }

        // Set value programmatically
        function setValue(date, hour) {
            state.selectedDate = date || '';
            state.selectedHour = hour || '';
            updateInputDisplay();
            if (state.isOpen) {
                render();
            }
        }

        // Event listeners
        btn.addEventListener('click', function(e) {
            e.stopPropagation();
            toggle();
        });

        input.addEventListener('input', function() {
            parseInputValue();
            triggerChange();
        });

        input.addEventListener('change', function() {
            parseInputValue();
            triggerChange();
        });

        // Close when clicking outside
        document.addEventListener('click', function(e) {
            if (state.isOpen && !container.contains(e.target)) {
                close();
            }
        });

        // Return public API
        return {
            open: open,
            close: close,
            toggle: toggle,
            getValue: getValue,
            setValue: setValue,
            getInput: function() { return input; }
        };
    }

    // Export to global scope
    window.FlowlordDateTimePicker = {
        create: create
    };
})();

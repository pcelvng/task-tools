// Calendar and Date Picker functionality
(function() {
    'use strict';

    function initCalendar(datesWithData) {
        const datePicker = document.getElementById('datePicker');
        const todayBtn = document.getElementById('todayBtn');
        
        if (!datePicker) return;
        
        // Convert array to Set for faster lookup
        const datesSet = new Set(datesWithData || []);
        
        // Set today's date as default if no date is provided
        if (!datePicker.value) {
            const today = new Date().toISOString().split('T')[0];
            datePicker.value = today;
        }
        
        // Update data indicator
        function updateDataIndicator(date) {
            if (datesSet.has(date)) {
                datePicker.classList.add('has-data');
            } else {
                datePicker.classList.remove('has-data');
            }
        }
        
        // Initial indicator update
        updateDataIndicator(datePicker.value);
        
        // Create custom date picker dropdown
        const dropdown = document.createElement('div');
        dropdown.id = 'dateDropdown';
        dropdown.className = 'date-dropdown';
        dropdown.style.display = 'none';
        
        // Generate calendar for current month and surrounding dates
        function generateCalendar() {
            const currentDate = datePicker.value ? new Date(datePicker.value + 'T12:00:00') : new Date();
            const year = currentDate.getFullYear();
            const month = currentDate.getMonth();
            
            let html = '<div class="calendar-header">';
            html += '<button type="button" class="month-nav" data-dir="-1">◀</button>';
            html += '<span class="month-label">' + currentDate.toLocaleDateString('en-US', {month: 'long', year: 'numeric'}) + '</span>';
            html += '<button type="button" class="month-nav" data-dir="1">▶</button>';
            html += '</div>';
            
            html += '<div class="calendar-grid">';
            html += '<div class="day-header">Su</div><div class="day-header">Mo</div><div class="day-header">Tu</div>';
            html += '<div class="day-header">We</div><div class="day-header">Th</div><div class="day-header">Fr</div><div class="day-header">Sa</div>';
            
            const firstDay = new Date(year, month, 1);
            const lastDay = new Date(year, month + 1, 0);
            const startDay = firstDay.getDay();
            
            // Add empty cells for days before month starts
            for (let i = 0; i < startDay; i++) {
                html += '<div class="day-cell empty"></div>';
            }
            
            // Add days of month
            for (let day = 1; day <= lastDay.getDate(); day++) {
                const dateStr = year + '-' + String(month + 1).padStart(2, '0') + '-' + String(day).padStart(2, '0');
                const hasData = datesSet.has(dateStr);
                const isSelected = dateStr === datePicker.value;
                
                let classes = 'day-cell';
                if (hasData) classes += ' has-data';
                if (isSelected) classes += ' selected';
                
                html += '<div class="' + classes + '" data-date="' + dateStr + '">' + day + '</div>';
            }
            
            html += '</div>';
            dropdown.innerHTML = html;
        }
        
        // Toggle dropdown
        datePicker.addEventListener('click', function(e) {
            e.stopPropagation();
            if (dropdown.style.display === 'none') {
                generateCalendar();
                dropdown.style.display = 'block';
                
                // Position dropdown
                const rect = datePicker.getBoundingClientRect();
                dropdown.style.position = 'absolute';
                dropdown.style.top = (rect.bottom + 5) + 'px';
                dropdown.style.left = rect.left + 'px';
            } else {
                dropdown.style.display = 'none';
            }
        });
        
        // Handle dropdown clicks
        dropdown.addEventListener('click', function(e) {
            e.stopPropagation();
            
            if (e.target.classList.contains('day-cell') && !e.target.classList.contains('empty')) {
                const selectedDate = e.target.getAttribute('data-date');
                datePicker.value = selectedDate;
                updateDataIndicator(selectedDate);
                dropdown.style.display = 'none';
                
                // Navigate to date
                navigateToDate(selectedDate);
            } else if (e.target.classList.contains('month-nav')) {
                const dir = parseInt(e.target.getAttribute('data-dir'));
                const currentDate = new Date(datePicker.value + 'T12:00:00');
                currentDate.setMonth(currentDate.getMonth() + dir);
                datePicker.value = currentDate.toISOString().split('T')[0];
                generateCalendar();
            }
        });
        
        // Close dropdown when clicking outside
        document.addEventListener('click', function() {
            dropdown.style.display = 'none';
        });
        
        // Handle today button
        if (todayBtn) {
            todayBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                const today = new Date().toISOString().split('T')[0];
                datePicker.value = today;
                updateDataIndicator(today);
                dropdown.style.display = 'none';
                navigateToDate(today);
            });
        }
        
        // Navigate to selected date
        function navigateToDate(selectedDate) {
            const currentUrl = new URL(window.location);
            const currentPage = currentUrl.pathname;
            
            const newUrl = new URL(currentPage, window.location.origin);
            newUrl.searchParams.set('date', selectedDate);
            
            // Preserve other query parameters
            const otherParams = ['type', 'job', 'result', 'sort', 'direction'];
            otherParams.forEach(param => {
                if (currentUrl.searchParams.has(param)) {
                    newUrl.searchParams.set(param, currentUrl.searchParams.get(param));
                }
            });
            
            window.location.href = newUrl.toString();
        }
        
        // Append dropdown to body
        document.body.appendChild(dropdown);
    }

    // Export to global scope
    window.FlowlordCalendar = {
        init: initCalendar
    };
})();


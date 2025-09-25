// Schedule Editor JavaScript - Drag and Drop Functionality

class ScheduleEditor {
    constructor() {
        this.currentSchedule = [];
        this.availableJobs = [];
        this.techId = null;
        this.weekStart = null;
        this.undoStack = [];
        this.redoStack = [];
        this.draggedJob = null;
        this.techCapacity = { daily: 8, weekly: 40 };

        this.init();
    }

    init() {
        this.setupEventListeners();
        this.loadTechnicians();
        this.setDefaultWeek();
    }

    setupEventListeners() {
        // Control buttons
        document.getElementById('load-schedule').addEventListener('click', () => this.loadSchedule());
        document.getElementById('save-btn').addEventListener('click', () => this.saveChanges());
        document.getElementById('cancel-btn').addEventListener('click', () => this.cancelChanges());
        document.getElementById('undo-btn').addEventListener('click', () => this.undo());
        document.getElementById('redo-btn').addEventListener('click', () => this.redo());

        // Navigation
        document.getElementById('prev-week').addEventListener('click', () => this.changeWeek(-7));
        document.getElementById('next-week').addEventListener('click', () => this.changeWeek(7));

        // Filters
        document.getElementById('job-search').addEventListener('input', (e) => this.filterJobs(e.target.value));
        document.getElementById('filter-urgent').addEventListener('change', () => this.applyFilters());
        document.getElementById('filter-night').addEventListener('change', () => this.applyFilters());
        document.getElementById('filter-region').addEventListener('change', () => this.applyFilters());

        // Modal
        document.querySelector('.close-modal').addEventListener('click', () => this.closeModal());
        document.getElementById('force-schedule').addEventListener('click', () => this.forceSchedule());
        document.getElementById('resolve-conflict').addEventListener('click', () => this.resolveConflict());
        document.getElementById('cancel-move').addEventListener('click', () => this.cancelMove());

        // Setup drop zones
        this.setupDropZones();
    }

    setupDropZones() {
        const daySchedules = document.querySelectorAll('.day-schedule');

        daySchedules.forEach(schedule => {
            // Create hourly drop zones
            for (let hour = 6; hour < 21; hour++) {
                const dropZone = document.createElement('div');
                dropZone.className = 'drop-zone';
                dropZone.style.top = `${(hour - 6) * 60}px`;
                dropZone.dataset.hour = hour;

                dropZone.addEventListener('dragover', (e) => this.handleDragOver(e));
                dropZone.addEventListener('drop', (e) => this.handleDrop(e));
                dropZone.addEventListener('dragleave', (e) => this.handleDragLeave(e));

                schedule.appendChild(dropZone);
            }
        });
    }

    async loadTechnicians() {
        try {
            const response = await fetch('/technicians');
            const data = await response.json();

            const select = document.getElementById('tech-select');
            select.innerHTML = '<option value="">Select Technician...</option>';

            data.technicians.forEach(tech => {
                const option = document.createElement('option');
                option.value = tech.technician_id;
                option.textContent = `${tech.name} (${tech.regions?.join(', ') || 'No regions'})`;
                select.appendChild(option);
            });
        } catch (error) {
            console.error('Error loading technicians:', error);
        }
    }

    setDefaultWeek() {
        const today = new Date();
        const monday = new Date(today);
        monday.setDate(today.getDate() - today.getDay() + 1);

        document.getElementById('week-start').value = monday.toISOString().split('T')[0];
        this.updateWeekDisplay();
    }

    updateWeekDisplay() {
        const weekStart = new Date(document.getElementById('week-start').value);
        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekStart.getDate() + 6);

        const options = { month: 'short', day: 'numeric', year: 'numeric' };
        document.getElementById('week-range').textContent =
            `${weekStart.toLocaleDateString('en-US', options)} - ${weekEnd.toLocaleDateString('en-US', options)}`;
    }

    async loadSchedule() {
        const techId = document.getElementById('tech-select').value;
        const weekStart = document.getElementById('week-start').value;

        if (!techId || !weekStart) {
            alert('Please select a technician and week');
            return;
        }

        this.techId = techId;
        this.weekStart = weekStart;

        // Load existing schedule
        await this.loadExistingSchedule();

        // Load available jobs
        await this.loadAvailableJobs();

        // Update capacity display
        this.updateCapacityDisplay();
    }

    async loadExistingSchedule() {
        try {
            const weekEnd = new Date(this.weekStart);
            weekEnd.setDate(weekEnd.getDate() + 6);

            const response = await fetch(
                `/schedule/existing?start=${this.weekStart}&end=${weekEnd.toISOString().split('T')[0]}&technician_ids=${this.techId}`
            );
            const data = await response.json();

            this.currentSchedule = data.rows;
            this.renderSchedule();
        } catch (error) {
            console.error('Error loading schedule:', error);
        }
    }

    async loadAvailableJobs() {
        try {
            const response = await fetch(
                `/jobs/search?tech_id=${this.techId}&radius_miles=100&due_within_days=30&limit=100`,
                { headers: { 'X-API-Key': localStorage.getItem('apiKey') || '' } }
            );
            const jobs = await response.json();

            this.availableJobs = jobs;
            this.renderAvailableJobs();
        } catch (error) {
            console.error('Error loading jobs:', error);
        }
    }

    renderSchedule() {
        // Clear existing schedule display
        document.querySelectorAll('.scheduled-job').forEach(el => el.remove());

        this.currentSchedule.forEach(job => {
            const jobDate = new Date(job.date);
            const dayIndex = (jobDate.getDay() + 6) % 7; // Convert to Mon=0
            const dayColumn = document.querySelector(`.day-column[data-day="${dayIndex}"] .day-schedule`);

            if (dayColumn) {
                const jobEl = this.createScheduledJobElement(job);
                dayColumn.appendChild(jobEl);
            }
        });
    }

    createScheduledJobElement(job) {
        const el = document.createElement('div');
        el.className = 'scheduled-job';
        el.draggable = true;
        el.dataset.workOrder = job.work_order;
        el.dataset.date = job.date;

        // Calculate position based on time
        const startTime = job.start_time || '09:00';
        const [hours, minutes] = startTime.split(':').map(Number);
        const topPosition = ((hours - 6) * 60) + minutes;
        el.style.top = `${topPosition}px`;

        // Calculate height based on duration
        const duration = job.est_hours || 2;
        el.style.height = `${duration * 60 - 10}px`;

        el.innerHTML = `
            <div class="job-time">${startTime} - ${this.calculateEndTime(startTime, duration)}</div>
            <div class="job-title">WO ${job.work_order}: ${job.site_name || 'Unknown Site'}</div>
        `;

        // Add drag handlers
        el.addEventListener('dragstart', (e) => this.handleDragStart(e, job));
        el.addEventListener('dragend', (e) => this.handleDragEnd(e));
        el.addEventListener('click', () => this.showJobDetails(job));

        return el;
    }

    renderAvailableJobs() {
        const container = document.getElementById('available-jobs');
        container.innerHTML = '';

        this.availableJobs.forEach(job => {
            const jobCard = this.createJobCard(job);
            container.appendChild(jobCard);
        });
    }

    createJobCard(job) {
        const card = document.createElement('div');
        card.className = 'job-card';
        card.draggable = true;
        card.dataset.workOrder = job.work_order;

        const priority = this.getJobPriority(job);
        const priorityClass = priority === 'Urgent' ? 'priority-urgent' :
                            priority === 'High' ? 'priority-high' : 'priority-normal';

        card.innerHTML = `
            <div class="job-card-header">
                <span class="job-wo">WO ${job.work_order}</span>
                <span class="job-priority ${priorityClass}">${priority}</span>
            </div>
            <div class="job-site">${job.site_name}</div>
            <div class="job-site">${job.city}, ${job.state}</div>
            <div class="job-meta">
                <span>Due: ${new Date(job.due_date).toLocaleDateString()}</span>
                <span>${job.est_hours || 2} hrs</span>
                ${job.is_night ? '<span class="night-badge">NIGHT</span>' : ''}
            </div>
        `;

        card.addEventListener('dragstart', (e) => this.handleDragStart(e, job));
        card.addEventListener('dragend', (e) => this.handleDragEnd(e));
        card.addEventListener('click', () => this.showJobDetails(job));

        return card;
    }

    getJobPriority(job) {
        if (job.jp_priority === 'NOV' || job.jp_priority === 'Urgent') return 'Urgent';
        if (job.days_til_due < 7) return 'High';
        return 'Normal';
    }

    handleDragStart(e, job) {
        this.draggedJob = job;
        e.target.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
    }

    handleDragEnd(e) {
        e.target.classList.remove('dragging');
        document.querySelectorAll('.drop-zone').forEach(zone => {
            zone.classList.remove('drag-over', 'invalid');
        });
    }

    handleDragOver(e) {
        e.preventDefault();
        const dropZone = e.target.closest('.drop-zone');
        if (dropZone) {
            dropZone.classList.add('drag-over');

            // Check for conflicts
            if (this.checkConflicts(dropZone)) {
                dropZone.classList.add('invalid');
            }
        }
    }

    handleDragLeave(e) {
        const dropZone = e.target.closest('.drop-zone');
        if (dropZone) {
            dropZone.classList.remove('drag-over', 'invalid');
        }
    }

    async handleDrop(e) {
        e.preventDefault();
        const dropZone = e.target.closest('.drop-zone');

        if (!dropZone || !this.draggedJob) return;

        dropZone.classList.remove('drag-over', 'invalid');

        const dayColumn = dropZone.closest('.day-column');
        const dayIndex = parseInt(dayColumn.dataset.day);
        const hour = parseInt(dropZone.dataset.hour);

        // Calculate the date
        const scheduleDate = new Date(this.weekStart);
        scheduleDate.setDate(scheduleDate.getDate() + dayIndex);

        // Check for conflicts
        const conflicts = await this.checkScheduleConflicts(
            this.draggedJob.work_order,
            scheduleDate,
            hour
        );

        if (conflicts.length > 0) {
            this.showConflictModal(conflicts, () => {
                this.addJobToSchedule(this.draggedJob, scheduleDate, hour);
            });
        } else {
            this.addJobToSchedule(this.draggedJob, scheduleDate, hour);
        }

        this.draggedJob = null;
    }

    async checkScheduleConflicts(workOrder, date, hour) {
        try {
            const response = await fetch('/schedule/conflicts', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-Key': localStorage.getItem('apiKey') || ''
                },
                body: JSON.stringify({
                    tech_id: this.techId,
                    date: date.toISOString().split('T')[0],
                    work_orders: [workOrder]
                })
            });

            const result = await response.json();
            return result.conflicts;
        } catch (error) {
            console.error('Error checking conflicts:', error);
            return [];
        }
    }

    addJobToSchedule(job, date, hour) {
        // Save state for undo
        this.saveState();

        // Add to schedule
        const scheduledJob = {
            work_order: job.work_order,
            technician_id: this.techId,
            date: date.toISOString().split('T')[0],
            start_time: `${hour.toString().padStart(2, '0')}:00`,
            est_hours: job.est_hours || 2,
            site_name: job.site_name,
            site_city: job.city,
            site_state: job.state
        };

        this.currentSchedule.push(scheduledJob);

        // Remove from available jobs
        this.availableJobs = this.availableJobs.filter(j => j.work_order !== job.work_order);

        // Re-render
        this.renderSchedule();
        this.renderAvailableJobs();
        this.updateCapacityDisplay();

        // Enable save button
        document.getElementById('save-btn').disabled = false;
    }

    showJobDetails(job) {
        const detailsEl = document.getElementById('job-details');

        detailsEl.innerHTML = `
            <div class="detail-row">
                <span class="detail-label">Work Order:</span>
                <span class="detail-value">${job.work_order}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Site:</span>
                <span class="detail-value">${job.site_name || 'Unknown'}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Location:</span>
                <span class="detail-value">${job.city || job.site_city}, ${job.state || job.site_state}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Due Date:</span>
                <span class="detail-value">${new Date(job.due_date).toLocaleDateString()}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Est. Hours:</span>
                <span class="detail-value">${job.est_hours || 2} hours</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Priority:</span>
                <span class="detail-value">${job.jp_priority || 'Normal'}</span>
            </div>
            ${job.is_night ? `
            <div class="detail-row">
                <span class="detail-label">Type:</span>
                <span class="detail-value">Night Job</span>
            </div>` : ''}
        `;
    }

    updateCapacityDisplay() {
        // Calculate daily capacities
        const dailyHours = {};
        const weekStart = new Date(this.weekStart);

        for (let i = 0; i < 7; i++) {
            const date = new Date(weekStart);
            date.setDate(weekStart.getDate() + i);
            const dateStr = date.toISOString().split('T')[0];

            dailyHours[dateStr] = this.currentSchedule
                .filter(job => job.date === dateStr)
                .reduce((sum, job) => sum + (job.est_hours || 2), 0);
        }

        // Update daily capacity bars
        const dailyCapEl = document.getElementById('daily-capacity');
        dailyCapEl.innerHTML = Object.entries(dailyHours).map(([date, hours]) => {
            const percentage = (hours / this.techCapacity.daily) * 100;
            const fillClass = percentage > 100 ? 'danger' : percentage > 80 ? 'warning' : '';

            return `
                <div class="capacity-day">
                    <div class="capacity-date">${new Date(date).toLocaleDateString('en-US', { weekday: 'short' })}</div>
                    <div class="capacity-bar">
                        <div class="capacity-fill ${fillClass}" style="width: ${Math.min(percentage, 100)}%"></div>
                    </div>
                    <div class="capacity-text">${hours.toFixed(1)} / ${this.techCapacity.daily} hrs</div>
                </div>
            `;
        }).join('');

        // Update weekly summary
        const weeklyTotal = Object.values(dailyHours).reduce((sum, hours) => sum + hours, 0);
        const weeklyPercentage = (weeklyTotal / this.techCapacity.weekly) * 100;
        const weeklyFillClass = weeklyPercentage > 100 ? 'danger' : weeklyPercentage > 80 ? 'warning' : '';

        const weeklySumEl = document.getElementById('weekly-summary');
        weeklySumEl.innerHTML = `
            <div class="capacity-bar">
                <div class="capacity-fill ${weeklyFillClass}" style="width: ${Math.min(weeklyPercentage, 100)}%"></div>
            </div>
            <div class="capacity-text">${weeklyTotal.toFixed(1)} / ${this.techCapacity.weekly} hrs (${weeklyPercentage.toFixed(0)}%)</div>
        `;
    }

    showConflictModal(conflicts, onResolve) {
        const modal = document.getElementById('conflict-modal');
        const detailsEl = document.getElementById('conflict-details');

        detailsEl.innerHTML = `
            <h4>The following conflicts were detected:</h4>
            <ul>
                ${conflicts.map(c => `
                    <li>
                        <strong>WO ${c.work_order}:</strong>
                        ${c.errors.join(', ')}
                    </li>
                `).join('')}
            </ul>
        `;

        this.conflictResolveCallback = onResolve;
        modal.style.display = 'flex';
    }

    closeModal() {
        document.getElementById('conflict-modal').style.display = 'none';
    }

    forceSchedule() {
        if (this.conflictResolveCallback) {
            this.conflictResolveCallback();
        }
        this.closeModal();
    }

    resolveConflict() {
        // Implement smart conflict resolution
        alert('Automatic conflict resolution coming soon!');
        this.closeModal();
    }

    cancelMove() {
        this.closeModal();
    }

    saveState() {
        this.undoStack.push({
            schedule: [...this.currentSchedule],
            availableJobs: [...this.availableJobs]
        });

        // Clear redo stack on new action
        this.redoStack = [];

        // Limit undo stack size
        if (this.undoStack.length > 50) {
            this.undoStack.shift();
        }

        // Update button states
        document.getElementById('undo-btn').disabled = false;
        document.getElementById('redo-btn').disabled = true;
    }

    undo() {
        if (this.undoStack.length === 0) return;

        const currentState = {
            schedule: [...this.currentSchedule],
            availableJobs: [...this.availableJobs]
        };

        this.redoStack.push(currentState);

        const previousState = this.undoStack.pop();
        this.currentSchedule = previousState.schedule;
        this.availableJobs = previousState.availableJobs;

        this.renderSchedule();
        this.renderAvailableJobs();
        this.updateCapacityDisplay();

        // Update button states
        document.getElementById('undo-btn').disabled = this.undoStack.length === 0;
        document.getElementById('redo-btn').disabled = false;
    }

    redo() {
        if (this.redoStack.length === 0) return;

        this.saveState();

        const nextState = this.redoStack.pop();
        this.currentSchedule = nextState.schedule;
        this.availableJobs = nextState.availableJobs;

        this.renderSchedule();
        this.renderAvailableJobs();
        this.updateCapacityDisplay();

        // Update button states
        document.getElementById('redo-btn').disabled = this.redoStack.length === 0;
    }

    async saveChanges() {
        if (!confirm('Are you sure you want to save these changes?')) return;

        try {
            // Prepare schedule updates
            const updates = this.currentSchedule.map(job => ({
                work_order: job.work_order,
                technician_id: job.technician_id,
                date: job.date,
                start_time: job.start_time,
                est_hours: job.est_hours
            }));

            const response = await fetch('/schedule/update', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-Key': localStorage.getItem('apiKey') || ''
                },
                body: JSON.stringify({
                    tech_id: this.techId,
                    week_start: this.weekStart,
                    updates: updates
                })
            });

            if (response.ok) {
                alert('Schedule saved successfully!');
                document.getElementById('save-btn').disabled = true;
            } else {
                throw new Error('Failed to save schedule');
            }
        } catch (error) {
            console.error('Error saving schedule:', error);
            alert('Failed to save schedule. Please try again.');
        }
    }

    cancelChanges() {
        if (!confirm('Are you sure you want to discard all changes?')) return;

        // Reload the original schedule
        this.loadSchedule();
    }

    changeWeek(days) {
        const currentWeek = new Date(document.getElementById('week-start').value);
        currentWeek.setDate(currentWeek.getDate() + days);
        document.getElementById('week-start').value = currentWeek.toISOString().split('T')[0];
        this.updateWeekDisplay();

        // Reload if we have a technician selected
        if (this.techId) {
            this.loadSchedule();
        }
    }

    filterJobs(searchTerm) {
        const cards = document.querySelectorAll('.job-card');
        cards.forEach(card => {
            const text = card.textContent.toLowerCase();
            card.style.display = text.includes(searchTerm.toLowerCase()) ? 'block' : 'none';
        });
    }

    applyFilters() {
        const urgent = document.getElementById('filter-urgent').checked;
        const night = document.getElementById('filter-night').checked;
        const region = document.getElementById('filter-region').value;

        const filtered = this.availableJobs.filter(job => {
            if (urgent && !['NOV', 'Urgent'].includes(job.jp_priority)) return false;
            if (night && !job.is_night) return false;
            if (region && job.state !== region) return false;
            return true;
        });

        const container = document.getElementById('available-jobs');
        container.innerHTML = '';
        filtered.forEach(job => {
            container.appendChild(this.createJobCard(job));
        });
    }

    calculateEndTime(startTime, duration) {
        const [hours, minutes] = startTime.split(':').map(Number);
        const endHours = hours + Math.floor(duration);
        const endMinutes = minutes + (duration % 1) * 60;

        return `${endHours.toString().padStart(2, '0')}:${endMinutes.toString().padStart(2, '0')}`;
    }

    checkConflicts(dropZone) {
        // Quick visual check for overlapping jobs
        const dayColumn = dropZone.closest('.day-column');
        const existingJobs = dayColumn.querySelectorAll('.scheduled-job');
        const dropHour = parseInt(dropZone.dataset.hour);

        for (const job of existingJobs) {
            const jobTop = parseInt(job.style.top);
            const jobHeight = parseInt(job.style.height);
            const jobStartHour = Math.floor(jobTop / 60) + 6;
            const jobEndHour = Math.ceil((jobTop + jobHeight) / 60) + 6;

            if (dropHour >= jobStartHour && dropHour < jobEndHour) {
                return true;
            }
        }

        return false;
    }
}

// Initialize the editor when page loads
document.addEventListener('DOMContentLoaded', () => {
    window.scheduleEditor = new ScheduleEditor();
});
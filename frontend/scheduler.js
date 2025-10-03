// scheduler.js - UNIFIED SCHEDULER LOGIC
// ============================================================================
// STATE MANAGEMENT
// ============================================================================

const STATE = {
    jobs: [],
    techs: [],
    selectedTech: null,
    selectedWeekStart: null,
    currentSchedule: [],
    filters: {
        region: '',
        priority: '',
        urgency: '',
        search: ''
    },
    map: null,
    markers: {
        jobs: [],
        techs: []
    }
};

// ============================================================================
// API CALLS
// ============================================================================

const API = {
    BASE: '',
    KEY: localStorage.getItem('apiKey') || 'devkey123',

    async get(endpoint) {
        const res = await fetch(`${this.BASE}${endpoint}`, {
            headers: { 'X-API-Key': this.KEY }
        });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    async post(endpoint, data) {
        const res = await fetch(`${this.BASE}${endpoint}`, {
            method: 'POST',
            headers: {
                'X-API-Key': this.KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    async delete(endpoint) {
        const res = await fetch(`${this.BASE}${endpoint}`, {
            method: 'DELETE',
            headers: { 'X-API-Key': this.KEY }
        });
        if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
        return res.json();
    },

    // Specific endpoints
    getUnscheduledJobs: () => API.get('/api/jobs/unscheduled'),
    getTechnicians: () => API.get('/api/technicians/all'),
    getTechWeek: (techId, weekStart) => API.get(`/api/schedule/week?tech_id=${techId}&week_start=${weekStart}`),
    assignJob: (data) => API.post('/api/schedule/assign', data),
    removeJob: (workOrder) => API.delete(`/api/schedule/remove/${workOrder}`),
    getSuggestions: (techId, date) => API.get(`/api/schedule/suggestions?tech_id=${techId}&date=${date}`),
    optimizeDay: (techId, date) => API.post('/api/schedule/optimize-day', { technician_id: techId, date })
};

// ============================================================================
// MAP FUNCTIONS
// ============================================================================

const MapManager = {
    init() {
        STATE.map = L.map('map').setView([39.7392, -104.9903], 7);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap'
        }).addTo(STATE.map);
    },

    clearMarkers(type = 'all') {
        if (type === 'all' || type === 'jobs') {
            STATE.markers.jobs.forEach(m => STATE.map.removeLayer(m));
            STATE.markers.jobs = [];
        }
        if (type === 'all' || type === 'techs') {
            STATE.markers.techs.forEach(m => STATE.map.removeLayer(m));
            STATE.markers.techs = [];
        }
    },

    showJobs(jobs = STATE.jobs) {
        this.clearMarkers('jobs');

        jobs.forEach(job => {
            if (!job.latitude || !job.longitude) return;

            const color = this.getJobColor(job);
            const marker = L.circleMarker([job.latitude, job.longitude], {
                radius: 8,
                fillColor: color,
                color: '#fff',
                weight: 2,
                opacity: 1,
                fillOpacity: 0.8
            }).addTo(STATE.map);

            marker.bindPopup(`
                <strong>WO ${job.work_order}</strong><br>
                ${job.site_name}<br>
                ${job.site_city}, ${job.site_state}<br>
                <strong>${job.jp_priority}</strong><br>
                Due: ${new Date(job.due_date).toLocaleDateString()}<br>
                <button onclick="JobManager.showDetails(${job.work_order})">View Details</button>
            `);

            marker.on('click', () => JobManager.showDetails(job.work_order));

            STATE.markers.jobs.push(marker);
        });

        // Fit bounds if we have markers
        if (STATE.markers.jobs.length > 0) {
            const group = L.featureGroup(STATE.markers.jobs);
            STATE.map.fitBounds(group.getBounds().pad(0.1));
        }
    },

    showTechs(techs = STATE.techs) {
        this.clearMarkers('techs');

        techs.forEach(tech => {
            if (!tech.home_latitude || !tech.home_longitude) return;

            const marker = L.marker([tech.home_latitude, tech.home_longitude], {
                icon: L.divIcon({
                    className: 'tech-map-marker',
                    html: `<div style="background:#3b82f6;color:white;border-radius:50%;width:30px;height:30px;display:flex;align-items:center;justify-content:center;border:2px solid white;font-weight:bold;font-size:12px;">${tech.name.substring(0,2).toUpperCase()}</div>`
                })
            }).addTo(STATE.map);

            marker.bindPopup(`
                <strong>${tech.name}</strong><br>
                ${tech.regions?.join(', ') || 'No regions'}<br>
                ${tech.current_week_hours || 0}/${tech.max_weekly_hours || 40} hrs<br>
                <button onclick="ScheduleManager.selectTech(${tech.technician_id})">View Schedule</button>
            `);

            STATE.markers.techs.push(marker);
        });
    },

    getJobColor(job) {
        if (job.urgency === 'critical') return '#dc2626';
        if (job.jp_priority === 'NOV' || job.jp_priority === 'Urgent') return '#dc2626';
        if (job.jp_priority === 'Monthly O&M') return '#f59e0b';
        return '#10b981';
    }
};

// ============================================================================
// JOB POOL MANAGEMENT
// ============================================================================

const JobManager = {
    async load() {
        try {
            const data = await API.getUnscheduledJobs();
            STATE.jobs = data.jobs;
            
            // Update UI
            document.getElementById('job-count').textContent = `${data.count} jobs`;
            document.getElementById('total-hours').textContent = `${data.summary.total_hours.toFixed(0)} hrs`;
            
            this.render();
            MapManager.showJobs();
        } catch (error) {
            console.error('Failed to load jobs:', error);
            alert('Failed to load jobs. Check console for details.');
        }
    },

    render() {
        const container = document.getElementById('job-list');
        const filtered = this.getFilteredJobs();

        if (filtered.length === 0) {
            container.innerHTML = '<div class="loading">No jobs match filters</div>';
            return;
        }

        container.innerHTML = filtered.map(job => `
            <div class="job-card" onclick="JobManager.showDetails(${job.work_order})">
                <div class="job-card-header">
                    <span class="job-wo">WO ${job.work_order}</span>
                    <span class="job-priority priority-${this.getPriorityClass(job)}">${job.jp_priority || 'N/A'}</span>
                </div>
                <div class="job-site">${job.site_name}</div>
                <div class="job-location">${job.site_city}, ${job.site_state}</div>
                <div class="job-meta">
                    <span class="job-urgency urgency-${job.urgency}">${job.urgency}</span>
                    <span>Due: ${new Date(job.due_date).toLocaleDateString()}</span>
                    <span>${job.est_hours || 2} hrs</span>
                    <span>${job.eligible_tech_count} techs</span>
                </div>
            </div>
        `).join('');
    },

    getFilteredJobs() {
        return STATE.jobs.filter(job => {
            // Region filter
            if (STATE.filters.region && job.site_state !== STATE.filters.region) return false;
            
            // Priority filter
            if (STATE.filters.priority && job.jp_priority !== STATE.filters.priority) return false;
            
            // Urgency filter
            if (STATE.filters.urgency && job.urgency !== STATE.filters.urgency) return false;
            
            // Search filter
            if (STATE.filters.search) {
                const search = STATE.filters.search.toLowerCase();
                const searchable = `${job.work_order} ${job.site_name} ${job.site_city} ${job.site_state}`.toLowerCase();
                if (!searchable.includes(search)) return false;
            }
            
            return true;
        });
    },

    getPriorityClass(job) {
        const p = job.jp_priority || '';
        if (p === 'NOV' || p === 'Urgent') return 'urgent';
        if (p === 'Monthly O&M') return 'monthly';
        return 'normal';
    },

    async showDetails(workOrder) {
        const job = STATE.jobs.find(j => j.work_order === workOrder);
        if (!job) return;

        const modal = document.getElementById('job-modal');
        const details = document.getElementById('job-details');

        details.innerHTML = `
            <div style="display:grid;grid-template-columns:140px 1fr;gap:12px;">
                <strong>Work Order:</strong><span>${job.work_order}</span>
                <strong>Site:</strong><span>${job.site_name}</span>
                <strong>Location:</strong><span>${job.site_city}, ${job.site_state}</span>
                <strong>Due Date:</strong><span>${new Date(job.due_date).toLocaleDateString()}</span>
                <strong>Priority:</strong><span>${job.jp_priority}</span>
                <strong>Urgency:</strong><span class="job-urgency urgency-${job.urgency}">${job.urgency}</span>
                <strong>Est. Hours:</strong><span>${job.est_hours || 2} hours</span>
                <strong>Eligible Techs:</strong><span>${job.eligible_tech_count} technicians</span>
                <strong>Days Until Due:</strong><span>${job.days_until_due} days</span>
            </div>
        `;

        // Store current job for assignment
        modal.dataset.workOrder = workOrder;

        modal.style.display = 'flex';
    },

    closeModal() {
        document.getElementById('job-modal').style.display = 'none';
    }
};

// ============================================================================
// TECH ROSTER MANAGEMENT
// ============================================================================

const TechManager = {
    async load() {
        try {
            const data = await API.getTechnicians();
            STATE.techs = data.technicians;
            
            this.render();
            this.populateSelects();
            MapManager.showTechs();
        } catch (error) {
            console.error('Failed to load technicians:', error);
            alert('Failed to load technicians. Check console for details.');
        }
    },

    render() {
        const container = document.getElementById('tech-list');

        container.innerHTML = STATE.techs.map(tech => {
            const utilization = tech.utilization_percent || 0;
            const capacityClass = utilization > 90 ? 'danger' : utilization > 80 ? 'warning' : '';

            return `
                <div class="tech-card ${STATE.selectedTech === tech.technician_id ? 'selected' : ''}" 
                     onclick="ScheduleManager.selectTech(${tech.technician_id})">
                    <div class="tech-name">${tech.name}</div>
                    <div class="tech-regions">${tech.regions?.join(', ') || 'No regions'}</div>
                    <div class="tech-capacity-bar">
                        <div class="tech-capacity-fill ${capacityClass}" 
                             style="width: ${Math.min(utilization, 100)}%"></div>
                    </div>
                    <div class="tech-stats">
                        <span>${tech.current_week_hours || 0}/${tech.max_weekly_hours || 40} hrs</span>
                        <span>${utilization.toFixed(0)}% utilized</span>
                    </div>
                </div>
            `;
        }).join('');
    },

    populateSelects() {
        const selects = [
            document.getElementById('tech-select'),
            document.getElementById('assign-tech-select')
        ];

        selects.forEach(select => {
            select.innerHTML = '<option value="">Select technician...</option>' +
                STATE.techs.map(t => `
                    <option value="${t.technician_id}">${t.name} (${t.regions?.join(', ') || 'No regions'})</option>
                `).join('');
        });
    }
};

// ============================================================================
// SCHEDULE MANAGEMENT
// ============================================================================

const ScheduleManager = {
    async selectTech(techId) {
        STATE.selectedTech = techId;
        
        // Set default week to current week
        if (!STATE.selectedWeekStart) {
            const today = new Date();
            const monday = new Date(today);
            monday.setDate(today.getDate() - today.getDay() + 1);
            STATE.selectedWeekStart = monday.toISOString().split('T')[0];
        }

        await this.loadWeek();
        TechManager.render();
    },

    async loadWeek() {
        if (!STATE.selectedTech || !STATE.selectedWeekStart) return;

        try {
            const data = await API.getTechWeek(STATE.selectedTech, STATE.selectedWeekStart);
            STATE.currentSchedule = data.days;
            
            this.renderCalendar();
            this.updateCapacityBadge();
        } catch (error) {
            console.error('Failed to load schedule:', error);
        }
    },

    renderCalendar() {
        const container = document.getElementById('calendar-view');
        const tech = STATE.techs.find(t => t.technician_id === STATE.selectedTech);

        if (!tech) {
            container.innerHTML = '<div class="calendar-placeholder">Select a technician</div>';
            return;
        }

        // Update week display
        const weekStart = new Date(STATE.selectedWeekStart);
        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekStart.getDate() + 6);
        document.getElementById('week-display').textContent = 
            `${weekStart.toLocaleDateString()} - ${weekEnd.toLocaleDateString()}`;

        // Render days
        container.innerHTML = STATE.currentSchedule.map(day => {
            const dayDate = new Date(day.date);
            const isWeekday = dayDate.getDay() >= 1 && dayDate.getDay() <= 5;

            if (!isWeekday) return ''; // Skip weekends for now

            const dailyMax = tech.max_daily_hours || 8;
            const hoursPercent = (day.total_hours / dailyMax) * 100;
            const hoursClass = hoursPercent > 100 ? 'danger' : hoursPercent > 80 ? 'warning' : '';

            return `
                <div class="day-column">
                    <div class="day-header">
                        <div>
                            <strong>${day.day_name}</strong>
                            <div class="day-date">${dayDate.toLocaleDateString()}</div>
                        </div>
                        <div class="day-hours ${hoursClass}">
                            ${day.total_hours.toFixed(1)}/${dailyMax} hrs
                        </div>
                    </div>
                    <div class="day-jobs">
                        ${day.jobs.map(job => `
                            <div class="scheduled-job" onclick="ScheduleManager.showJobOptions(${job.work_order})">
                                <div class="scheduled-job-time">${job.start_time || '09:00'}</div>
                                <div class="scheduled-job-title">WO ${job.work_order}</div>
                                <div class="scheduled-job-title">${job.site_name || 'Unknown'}</div>
                            </div>
                        `).join('')}
                        <div class="add-job-zone" onclick="AssignmentManager.openForDay('${day.date}')">
                            + Add Job
                        </div>
                    </div>
                </div>
            `;
        }).join('');
    },

    updateCapacityBadge() {
        const tech = STATE.techs.find(t => t.technician_id === STATE.selectedTech);
        if (!tech) return;

        const weeklyHours = STATE.currentSchedule.reduce((sum, day) => sum + day.total_hours, 0);
        const maxWeekly = tech.max_weekly_hours || 40;
        const percent = (weeklyHours / maxWeekly) * 100;

        const badge = document.getElementById('tech-capacity');
        badge.textContent = `${weeklyHours.toFixed(1)}/${maxWeekly} hrs`;
        badge.className = 'capacity-badge';
        if (percent > 100) badge.classList.add('danger');
        else if (percent > 80) badge.classList.add('warning');
    },

    async changeWeek(days) {
        const current = new Date(STATE.selectedWeekStart);
        current.setDate(current.getDate() + days);
        STATE.selectedWeekStart = current.toISOString().split('T')[0];
        await this.loadWeek();
    },

    async showJobOptions(workOrder) {
        if (confirm('Remove this job from schedule?')) {
            try {
                await API.removeJob(workOrder);
                await this.loadWeek();
                await JobManager.load(); // Refresh job pool
                alert('Job removed from schedule');
            } catch (error) {
                alert('Failed to remove job: ' + error.message);
            }
        }
    }
};

// ============================================================================
// ASSIGNMENT MANAGEMENT
// ============================================================================

const AssignmentManager = {
    openForDay(date) {
        if (!STATE.selectedTech) {
            alert('Please select a technician first');
            return;
        }

        const modal = document.getElementById('assign-modal');
        document.getElementById('assign-tech-select').value = STATE.selectedTech;
        document.getElementById('assign-date').value = date;
        document.getElementById('assign-time').value = '09:00';

        modal.style.display = 'flex';
    },

    openForJob() {
        const modal = document.getElementById('job-modal');
        const workOrder = modal.dataset.workOrder;
        
        if (!workOrder) return;

        modal.style.display = 'none';

        const assignModal = document.getElementById('assign-modal');
        const info = document.getElementById('assign-job-info');
        const job = STATE.jobs.find(j => j.work_order == workOrder);

        info.innerHTML = `<strong>Assigning:</strong> WO ${workOrder} - ${job?.site_name || 'Unknown'}`;
        assignModal.dataset.workOrder = workOrder;
        assignModal.style.display = 'flex';
    },

    async confirm() {
        const modal = document.getElementById('assign-modal');
        const workOrder = modal.dataset.workOrder || 
                         document.getElementById('job-modal').dataset.workOrder;
        const techId = parseInt(document.getElementById('assign-tech-select').value);
        const date = document.getElementById('assign-date').value;
        const time = document.getElementById('assign-time').value;

        if (!workOrder || !techId || !date) {
            alert('Please fill in all fields');
            return;
        }

        try {
            const result = await API.assignJob({
                work_order: parseInt(workOrder),
                technician_id: techId,
                date: date,
                start_time: time
            });

            if (result.success) {
                modal.style.display = 'none';
                alert('Job assigned successfully!');
                
                // Refresh data
                await JobManager.load();
                await TechManager.load();
                if (STATE.selectedTech) {
                    await ScheduleManager.loadWeek();
                }
            } else {
                const warnings = document.getElementById('validation-warnings');
                warnings.innerHTML = `
                    <strong>Validation Errors:</strong>
                    <ul>${result.errors.map(e => `<li>${e}</li>`).join('')}</ul>
                    ${result.warnings.length ? `<strong>Warnings:</strong><ul>${result.warnings.map(w => `<li>${w}</li>`).join('')}</ul>` : ''}
                `;
            }
        } catch (error) {
            alert('Failed to assign job: ' + error.message);
        }
    },

    close() {
        document.getElementById('assign-modal').style.display = 'none';
    }
};

// ============================================================================
// EVENT LISTENERS
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Initialize map
    MapManager.init();

    // Load initial data
    JobManager.load();
    TechManager.load();

    // Header buttons
    document.getElementById('refresh-btn').addEventListener('click', async () => {
        await JobManager.load();
        await TechManager.load();
    });

    // Filters
    document.getElementById('job-search').addEventListener('input', (e) => {
        STATE.filters.search = e.target.value;
        JobManager.render();
    });

    document.getElementById('filter-region').addEventListener('change', (e) => {
        STATE.filters.region = e.target.value;
        JobManager.render();
    });

    document.getElementById('filter-priority').addEventListener('change', (e) => {
        STATE.filters.priority = e.target.value;
        JobManager.render();
    });

    document.getElementById('filter-urgency').addEventListener('change', (e) => {
        STATE.filters.urgency = e.target.value;
        JobManager.render();
    });

    // Tech selector
    document.getElementById('tech-select').addEventListener('change', (e) => {
        if (e.target.value) {
            ScheduleManager.selectTech(parseInt(e.target.value));
        }
    });

    // Week navigation
    document.getElementById('prev-week').addEventListener('click', () => {
        ScheduleManager.changeWeek(-7);
    });

    document.getElementById('next-week').addEventListener('click', () => {
        ScheduleManager.changeWeek(7);
    });

    // Modal buttons
    document.getElementById('assign-job-btn').addEventListener('click', () => {
        AssignmentManager.openForJob();
    });

    document.getElementById('confirm-assign-btn').addEventListener('click', () => {
        AssignmentManager.confirm();
    });

    // Close modals
    document.querySelectorAll('.close-modal').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.target.closest('.modal').style.display = 'none';
        });
    });

    // Map controls
    document.getElementById('show-all-jobs').addEventListener('click', () => {
        MapManager.showJobs();
    });

    document.getElementById('show-techs').addEventListener('click', () => {
        MapManager.showTechs();
    });
});

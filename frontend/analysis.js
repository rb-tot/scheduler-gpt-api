// Monthly Analysis JavaScript
const API_BASE = '/';
const API_KEY = 'devkey123';

// Loading function
function showLoading() {
    // Update summary cards to show loading
    const summaryCards = document.getElementById('summary-cards');
    if (summaryCards) {
        summaryCards.innerHTML = '<div class="loading">Loading summary data...</div>';
    }
    
    const problemCounts = document.getElementById('problem-counts');
    if (problemCounts) {
        problemCounts.innerHTML = '<div class="loading">Analyzing...</div>';
    }
    
    const weekGrid = document.getElementById('week-grid');
    if (weekGrid) {
        weekGrid.innerHTML = '<div class="loading">Processing...</div>';
    }
    
    const problemList = document.getElementById('problem-list');
    if (problemList) {
        problemList.innerHTML = '<div class="loading">Finding problem jobs...</div>';
    }
}

async function runAnalysis() {
    const monthSelect = document.getElementById('month-select');
    const month = monthSelect.value;
    const year = 2025;

    showLoading();

    try {
        const response = await fetch(`${API_BASE}analysis/monthly?year=${year}&month=${month}`, {
            headers: {
                'X-API-Key': API_KEY
            }
        });

        if (!response.ok) {
            throw new Error(`Analysis failed: ${response.statusText}`);
        }

        const data = await response.json();
        displayResults(data);
    } catch (error) {
        console.error('Analysis error:', error);
        alert('Failed to run analysis: ' + error.message);
    }
}

function displayResults(data) {
    // Make sure the elements exist before updating them
    const totalJobs = document.getElementById('total-jobs');
    if (totalJobs) {
        totalJobs.textContent = data.summary.total_jobs;
    }

    // Summary cards - check if element exists
    const summaryCards = document.getElementById('summary-cards');
    if (summaryCards) {
        summaryCards.innerHTML = `
            <div class="card">
                <h3>Total Jobs</h3>
                <div class="big-number">${data.summary.total_jobs}</div>
                <small>${data.summary.total_job_hours || 0} hours needed</small>
            </div>
            <div class="card">
                <h3>Tech Capacity</h3>
                <div class="big-number">${data.summary.tech_count || 0} techs</div>
                <small>${data.summary.total_tech_capacity || 0} hours available</small>
            </div>
            <div class="card ${(data.summary.utilization_percent || 0) > 100 ? 'problem' : ''}">
                <h3>Utilization</h3>
                <div class="big-number">${data.summary.utilization_percent || 0}%</div>
                <small>${(data.summary.utilization_percent || 0) > 100 ? '⚠️ OVERBOOKED' : '✅ Manageable'}</small>
            </div>
        `;
    }

    // Problem counts
    const problemCounts = document.getElementById('problem-counts');
    if (problemCounts && data.summary.problem_jobs_count) {
        const problemHtml = `
            <div class="problem-item">
                <span class="label">Remote Jobs (>100mi):</span>
                <span class="count">${data.summary.problem_jobs_count.remote || 0}</span>
            </div>
            <div class="problem-item">
                <span class="label">Limited Techs (≤2):</span>
                <span class="count">${data.summary.problem_jobs_count.limited_techs || 0}</span>
            </div>
            <div class="problem-item">
                <span class="label">Night Jobs:</span>
                <span class="count">${data.summary.problem_jobs_count.night || 0}</span>
            </div>
            <div class="problem-item">
                <span class="label">Friday Restricted:</span>
                <span class="count">${data.summary.problem_jobs_count.friday_restricted || 0}</span>
            </div>
        `;
        problemCounts.innerHTML = problemHtml;
    }

    // Weekly matrix
    const weekGrid = document.getElementById('week-grid');
    if (weekGrid && data.summary.weekly_summary) {
        let weekGridHtml = `
            <table class="week-table">
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Must Do</th>
                        <th>Should Do</th>
                        <th>Total Hours</th>
                        <th>Jobs</th>
                    </tr>
                </thead>
                <tbody>
        `;

        for (let i = 1; i <= 4; i++) {
            const week = data.summary.weekly_summary[`week_${i}`];
            if (week) {
                const total = week.must_do + week.should_do;
                weekGridHtml += `
                    <tr>
                        <td>Week ${i}</td>
                        <td class="must-do">${week.must_do || 0}</td>
                        <td class="should-do">${week.should_do || 0}</td>
                        <td>${week.total_hours || 0}h</td>
                        <td class="total">${week.job_count || total}</td>
                    </tr>
                `;
            }
        }
        weekGridHtml += '</tbody></table>';
        weekGrid.innerHTML = weekGridHtml;
    }

    // Problem details
    const problemList = document.getElementById('problem-list');
    if (problemList && data.problem_jobs) {
        let problemDetailsHtml = '';

        if (data.problem_jobs.remote_locations && data.problem_jobs.remote_locations.length > 0) {
            problemDetailsHtml += '<div class="problem-section"><h3>🚗 Remote Locations</h3>';
            data.problem_jobs.remote_locations.forEach(job => {
                problemDetailsHtml += `
                    <div class="problem-card">
                        <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                        <small>${job.location || ''} • ${job.est_hours || 2}h</small><br>
                        ${job.closest_techs ? `
                        <div class="closest-techs">
                            <strong>Closest Techs:</strong><br>
                            ${job.closest_techs.map(t => 
                                `• ${t.tech_name} (${t.home_location}): ${t.distance} mi`
                            ).join('<br>')}
                        </div>` : ''}
                    </div>`;
            });
            problemDetailsHtml += '</div>';
        }

        if (data.problem_jobs.limited_eligibility && data.problem_jobs.limited_eligibility.length > 0) {
            problemDetailsHtml += '<div class="problem-section"><h3>⚠️ Limited Technician Eligibility</h3>';
            data.problem_jobs.limited_eligibility.forEach(job => {
                problemDetailsHtml += `
                    <div class="problem-card">
                        <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                        <small>${job.est_hours || 2}h • Eligible: ${job.tech_names ? job.tech_names.join(', ') : 'Unknown'}</small>
                    </div>`;
            });
            problemDetailsHtml += '</div>';
        }

        problemList.innerHTML = problemDetailsHtml || '<p>No problem jobs identified!</p>';
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Set default month to September 2025 (where your jobs are)
    const monthSelect = document.getElementById('month-select');
    if (monthSelect) {
        monthSelect.value = 9;  // September
    }
});
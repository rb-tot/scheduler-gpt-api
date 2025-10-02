// Monthly Analysis JavaScript
const API_BASE = '/';
const API_KEY = 'devkey123';

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
    // Summary cards with capacity info
    document.getElementById('total-jobs').textContent = data.summary.total_jobs;
    
    // Add new capacity card
    const summaryCards = document.getElementById('summary-cards');
    summaryCards.innerHTML = `
        <div class="card">
            <h3>Total Jobs</h3>
            <div class="big-number">${data.summary.total_jobs}</div>
            <small>${data.summary.total_job_hours} hours needed</small>
        </div>
        <div class="card">
            <h3>Tech Capacity</h3>
            <div class="big-number">${data.summary.tech_count} techs</div>
            <small>${data.summary.total_tech_capacity} hours available</small>
        </div>
        <div class="card ${data.summary.utilization_percent > 100 ? 'problem' : ''}">
            <h3>Utilization</h3>
            <div class="big-number">${data.summary.utilization_percent}%</div>
            <small>${data.summary.utilization_percent > 100 ? '⚠️ OVERBOOKED' : '✅ Manageable'}</small>
        </div>
    `;

    // Problem counts
    const problemHtml = `
        <div class="problem-item">
            <span class="label">Remote Jobs (>100mi):</span>
            <span class="count">${data.summary.problem_jobs_count.remote}</span>
        </div>
        <div class="problem-item">
            <span class="label">Limited Techs (≤2):</span>
            <span class="count">${data.summary.problem_jobs_count.limited_techs}</span>
        </div>
        <div class="problem-item">
            <span class="label">Night Jobs:</span>
            <span class="count">${data.summary.problem_jobs_count.night}</span>
        </div>
        <div class="problem-item">
            <span class="label">Friday Restricted:</span>
            <span class="count">${data.summary.problem_jobs_count.friday_restricted}</span>
        </div>
    `;
    document.getElementById('problem-counts').innerHTML = problemHtml;

    // Enhanced weekly matrix with targets
    let weekGridHtml = `
        <table class="week-table">
            <thead>
                <tr>
                    <th>Week</th>
                    <th>Must Do</th>
                    <th>Should Do</th>
                    <th>Total Hours</th>
                    <th>Target Hours</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
    `;

    for (let i = 1; i <= 4; i++) {
        const week = data.summary.weekly_summary[`week_${i}`];
        const target = data.suggested_targets[`week_${i}`];
        const status = week.total_hours > target.capacity ? '🔴' : 
                       week.total_hours > target.target_hours ? '🟡' : '🟢';
        
        weekGridHtml += `
            <tr>
                <td>Week ${i}</td>
                <td class="must-do">${week.must_do}</td>
                <td class="should-do">${week.should_do}</td>
                <td>${week.total_hours}h</td>
                <td>${target.target_hours}h</td>
                <td>${status}</td>
            </tr>
        `;
    }
    weekGridHtml += '</tbody></table>';
    document.getElementById('week-grid').innerHTML = weekGridHtml;

    // Enhanced problem details with closest techs
    let problemDetailsHtml = '';

    if (data.problem_jobs.remote_locations.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>🚗 Remote Locations</h3>';
        data.problem_jobs.remote_locations.forEach(job => {
            problemDetailsHtml += `
                <div class="problem-card">
                    <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                    <small>${job.location} • ${job.est_hours}h</small><br>
                    <div class="closest-techs">
                        <strong>Closest Techs:</strong><br>
                        ${job.closest_techs.map(t => 
                            `• ${t.tech_name} (${t.home_location}): ${t.distance} mi`
                        ).join('<br>')}
                    </div>
                </div>`;
        });
        problemDetailsHtml += '</div>';
    }

    if (data.problem_jobs.limited_eligibility.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>⚠️ Limited Technician Eligibility</h3>';
        data.problem_jobs.limited_eligibility.forEach(job => {
            problemDetailsHtml += `
                <div class="problem-card">
                    <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                    <small>${job.est_hours}h • Eligible: ${job.tech_names.join(', ')}</small>
                </div>`;
        });
        problemDetailsHtml += '</div>';
    }

    document.getElementById('problem-list').innerHTML = problemDetailsHtml || '<p>No problem jobs identified!</p>';
}

// Add CSS for analysis page
const style = document.createElement('style');
style.textContent = `
    .card-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin: 20px 0;
    }

    .card {
        background: white;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    .card.problem {
        background: #fff3cd;
        border-left: 4px solid #ffc107;
    }

    .big-number {
        font-size: 48px;
        font-weight: bold;
        color: #0066cc;
    }

    .problem-item {
        display: flex;
        justify-content: space-between;
        padding: 8px 0;
        border-bottom: 1px solid #dee2e6;
    }

    .problem-item:last-child {
        border-bottom: none;
    }

    .week-table {
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
    }

    .week-table th,
    .week-table td {
        padding: 12px;
        text-align: center;
        border: 1px solid #dee2e6;
    }

    .week-table thead {
        background: #f8f9fa;
    }

    .must-do {
        background: #ffebee;
        font-weight: bold;
        color: #c62828;
    }

    .should-do {
        background: #e3f2fd;
        color: #1565c0;
    }

    .problem-section {
        margin: 20px 0;
        padding: 15px;
        background: #f8f9fa;
        border-radius: 8px;
    }

    .problem-section h3 {
        margin-top: 0;
        color: #495057;
    }

    .problem-section ul {
        list-style-type: none;
        padding: 0;
    }

    .problem-section li {
        padding: 8px 0;
        border-bottom: 1px solid #dee2e6;
    }

    .problem-section li:last-child {
        border-bottom: none;
    }

    .distance {
        color: #dc3545;
        font-style: italic;
    }

    .tech-count {
        color: #fd7e14;
        font-weight: bold;
    }

    .loading {
        padding: 20px;
        text-align: center;
        color: #6c757d;
        font-style: italic;
    }
`;
document.head.appendChild(style);

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Set default month to current month
    const now = new Date();
    const currentMonth = now.getMonth() + 1;
    document.getElementById('month-select').value = currentMonth;
});
// Monthly Analysis JavaScript
const API_BASE = '/';
const API_KEY = 'devkey123'

async function runAnalysis() {
    const monthSelect = document.getElementById('month-select');
    const month = monthSelect.value;
    const year = 2025;

    showLoading();

    try {
        const response = await fetch(`${API_BASE}analysis/monthly?year=${year}&month=${month}`, {
            headers: {
                'X-API-Key': API_KEY}
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

function showLoading() {
    document.getElementById('total-jobs').textContent = 'Loading...';
    document.getElementById('problem-counts').innerHTML = '<div class="loading">Analyzing...</div>';
    document.getElementById('week-grid').innerHTML = '<div class="loading">Processing...</div>';
    document.getElementById('problem-list').innerHTML = '';
}

function displayResults(data) {
    // Summary cards
    document.getElementById('total-jobs').textContent = data.summary.total_jobs;

    // Problem counts
    const problemHtml = `
        <div class="problem-item">
            <span class="label">Remote Jobs:</span>
            <span class="count">${data.summary.problem_jobs_count.remote}</span>
        </div>
        <div class="problem-item">
            <span class="label">Limited Techs:</span>
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

    // Weekly matrix
    let weekGridHtml = '<table class="week-table"><thead><tr><th>Week</th><th>Must Do</th><th>Should Do</th><th>Total</th></tr></thead><tbody>';

    for (let i = 1; i <= 4; i++) {
        const week = data.summary.weekly_summary[`week_${i}`];
        const total = week.must_do + week.should_do;
        weekGridHtml += `
            <tr>
                <td>Week ${i}</td>
                <td class="must-do">${week.must_do}</td>
                <td class="should-do">${week.should_do}</td>
                <td class="total">${total}</td>
            </tr>
        `;
    }
    weekGridHtml += '</tbody></table>';
    document.getElementById('week-grid').innerHTML = weekGridHtml;

    // Problem details
    let problemDetailsHtml = '';

    if (data.problem_jobs.remote_locations.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>🚗 Remote Locations (>100 miles)</h3><ul>';
        data.problem_jobs.remote_locations.forEach(job => {
            problemDetailsHtml += `
                <li>
                    <strong>WO ${job.work_order}:</strong> ${job.site_name}
                    <span class="distance">(${job.distance} miles)</span>
                </li>`;
        });
        problemDetailsHtml += '</ul></div>';
    }

    if (data.problem_jobs.limited_eligibility.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>⚠️ Limited Technician Eligibility</h3><ul>';
        data.problem_jobs.limited_eligibility.forEach(job => {
            problemDetailsHtml += `
                <li>
                    <strong>WO ${job.work_order}:</strong> ${job.site_name}
                    <span class="tech-count">(${job.eligible_techs} techs)</span>
                </li>`;
        });
        problemDetailsHtml += '</ul></div>';
    }

    if (data.problem_jobs.night_jobs.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>🌙 Night Jobs</h3><ul>';
        data.problem_jobs.night_jobs.forEach(job => {
            problemDetailsHtml += `<li><strong>WO ${job.work_order}:</strong> ${job.site_name}</li>`;
        });
        problemDetailsHtml += '</ul></div>';
    }

    if (data.problem_jobs.friday_restricted.length > 0) {
        problemDetailsHtml += '<div class="problem-section"><h3>📅 Friday Restricted Sites</h3><ul>';
        data.problem_jobs.friday_restricted.forEach(job => {
            problemDetailsHtml += `<li><strong>WO ${job.work_order}:</strong> ${job.site_name}</li>`;
        });
        problemDetailsHtml += '</ul></div>';
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
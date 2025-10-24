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
        const response = await fetch(`${API_BASE}api/analysis/monthly?year=${year}&month=${month}`, {
            headers: {
                'X-API-Key': API_KEY
            }
        });

        if (!response.ok) {
            throw new Error(`Analysis failed: ${response.statusText}`);
        }

        const data = await response.json();
        console.log('Analysis data received:', data);
        displayResults(data);
    } catch (error) {
        console.error('Analysis error:', error);
        alert('Failed to run analysis: ' + error.message);
    }
}

function displayResults(data) {
    // Summary cards
    const summaryCards = document.getElementById('summary-cards');
    if (summaryCards) {
        const manageable = data.summary.is_manageable ? '✅ Manageable' : '⚠️ OVERBOOKED';
        const cardClass = data.summary.is_manageable ? '' : 'problem';
        
        summaryCards.innerHTML = `
            <div class="card">
                <h3>Total Jobs</h3>
                <div class="big-number">${data.summary.total_jobs}</div>
                <small>${data.summary.total_work_hours} work hours</small>
            </div>
            <div class="card">
                <h3>Drive Time</h3>
                <div class="big-number">${data.summary.total_drive_hours}h</div>
                <small>Estimated travel time</small>
            </div>
            <div class="card">
                <h3>Total Hours</h3>
                <div class="big-number">${data.summary.total_hours}h</div>
                <small>Work + Drive combined</small>
            </div>
            <div class="card">
                <h3>Tech Capacity</h3>
                <div class="big-number">${data.summary.tech_count} techs</div>
                <small>${data.summary.total_tech_capacity}h available</small>
            </div>
            <div class="card ${cardClass}">
                <h3>Utilization</h3>
                <div class="big-number">${data.summary.utilization_percent}%</div>
                <small>${manageable}</small>
            </div>
            <div class="card">
                <h3>Problem Jobs</h3>
                <div class="big-number">${data.summary.remote_jobs_count + data.summary.limited_eligibility_count}</div>
                <small>${data.summary.remote_jobs_count} remote, ${data.summary.limited_eligibility_count} limited techs</small>
            </div>
        `;
    }

    // Regional breakdown
    const regionalSection = document.getElementById('regional-breakdown');
    if (regionalSection && data.regional_breakdown) {
        let regionalHtml = '<h2>Regional Breakdown</h2>';
        regionalHtml += '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin-bottom: 30px;">';
        
        data.regional_breakdown.forEach(region => {
            regionalHtml += `
                <div class="card" style="padding: 15px;">
                    <h3 style="margin: 0 0 10px 0; font-size: 14px;">${region.region.replace(/_/g, ' ')}</h3>
                    <div style="font-size: 24px; font-weight: bold; color: #667eea;">${region.jobs} jobs</div>
                    <div style="font-size: 12px; color: #6c757d; margin-top: 5px;">
                        ${region.work_hours}h work + ${region.drive_hours}h drive<br>
                        <strong>${region.total_hours}h total</strong>
                    </div>
                </div>
            `;
        });
        
        regionalHtml += '</div>';
        regionalSection.innerHTML = regionalHtml;
    }

    // Weekly matrix with priorities
    const weekGrid = document.getElementById('week-grid');
    if (weekGrid && data.weekly_breakdown) {
        let weekGridHtml = `
            <table class="week-table">
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Date Range</th>
                        <th>Jobs</th>
                        <th>Work Hrs</th>
                        <th>Drive Hrs</th>
                        <th>Total Hrs</th>
                        <th>Urgent</th>
                        <th>Monthly</th>
                        <th>Annual</th>
                    </tr>
                </thead>
                <tbody>
        `;

        data.weekly_breakdown.forEach(week => {
            const overloaded = week.total_hours > 80 ? 'style="background: #fff3cd;"' : '';
            weekGridHtml += `
                <tr ${overloaded}>
                    <td><strong>Week ${week.week}</strong></td>
                    <td>${week.date_range}</td>
                    <td style="text-align: center;">${week.jobs}</td>
                    <td style="text-align: center;">${week.work_hours}h</td>
                    <td style="text-align: center; color: #6c757d;">${week.drive_hours}h</td>
                    <td style="text-align: center;"><strong>${week.total_hours}h</strong></td>
                    <td class="must-do">${week.urgent}</td>
                    <td class="should-do">${week.monthly}</td>
                    <td style="text-align: center; background: #e8f5e9;">${week.annual}</td>
                </tr>
            `;
        });
        
        weekGridHtml += '</tbody></table>';
        weekGrid.innerHTML = weekGridHtml;
    }

    // Problem details
    const problemList = document.getElementById('problem-list');
    if (problemList && data.problem_jobs) {
        let problemDetailsHtml = '';

        // Remote locations
        if (data.problem_jobs.remote_locations && data.problem_jobs.remote_locations.length > 0) {
            problemDetailsHtml += '<div class="problem-section"><h3>🚗 Remote Locations (>150 miles)</h3>';
            data.problem_jobs.remote_locations.forEach(job => {
                problemDetailsHtml += `
                    <div class="problem-card">
                        <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                        <small>Region: ${job.region.replace(/_/g, ' ')} • ${job.est_hours}h</small><br>
                        <small style="color: #dc3545;">
                            <strong>${job.distance_from_nearest} miles</strong> from ${job.nearest_tech}
                        </small>
                    </div>`;
            });
            problemDetailsHtml += '</div>';
        }

        // Limited eligibility
        if (data.problem_jobs.limited_eligibility && data.problem_jobs.limited_eligibility.length > 0) {
            problemDetailsHtml += '<div class="problem-section"><h3>⚠️ Limited Technician Eligibility (≤2 techs)</h3>';
            data.problem_jobs.limited_eligibility.forEach(job => {
                problemDetailsHtml += `
                    <div class="problem-card">
                        <strong>WO ${job.work_order}:</strong> ${job.site_name}<br>
                        <small>Region: ${job.region.replace(/_/g, ' ')} • ${job.est_hours}h</small><br>
                        <small style="color: #dc3545;">
                            Only ${job.eligible_techs} eligible tech(s): ${job.tech_names.join(', ')}
                        </small>
                    </div>`;
            });
            problemDetailsHtml += '</div>';
        }

        problemList.innerHTML = problemDetailsHtml || '<p style="color: #10b981; font-weight: bold;">✅ No problem jobs identified!</p>';
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Set default month to September 2025
    const monthSelect = document.getElementById('month-select');
    if (monthSelect) {
        monthSelect.value = 9;
    }
});
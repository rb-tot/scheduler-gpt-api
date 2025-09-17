// Configuration
const API_URL = ''; // Empty means same domain
const API_KEY = 'devkey123'; // Replace with your actual API key

// Map setup
let map;
let markers = [];
let selectedTech = null;
let jobsData = [];

// Initialize map
function initMap() {
    map = L.map('map').setView([39.7392, -104.9903], 10);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);
}

// Load technicians on page load
async function loadTechnicians() {
    try {
        const response = await fetch(`${API_URL}/technicians?active_only=true`, {
            headers: {'X-API-Key': API_KEY}
        });
        const data = await response.json();
        
        const select = document.getElementById('tech-select');
        data.technicians.forEach(tech => {
            const option = document.createElement('option');
            option.value = tech.technician_id;
            option.text = tech.name;
            option.dataset.lat = tech.home_latitude;
            option.dataset.lon = tech.home_longitude;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Failed to load technicians:', error);
    }
}

// Load jobs for selected tech and week
async function loadJobs() {
    const techSelect = document.getElementById('tech-select');
    const techId = techSelect.value;
    const weekStart = document.getElementById('week-start').value;
    
    if (!techId || !weekStart) {
        alert('Please select a technician and week start date');
        return;
    }
    
    // Get tech location
    const selectedOption = techSelect.options[techSelect.selectedIndex];
    const techLat = parseFloat(selectedOption.dataset.lat);
    const techLon = parseFloat(selectedOption.dataset.lon);
    
    try {
        // This calls your existing endpoint
        const response = await fetch(
            `${API_URL}/jobs/search?tech_id=${techId}&radius_miles=50&due_within_days=30&limit=50`,
            {headers: {'X-API-Key': API_KEY}}
        );
        jobsData = await response.json();
        
        displayJobs(jobsData, techLat, techLon);
        document.getElementById('job-count').textContent = jobsData.length;
    } catch (error) {
        console.error('Failed to load jobs:', error);
    }
}

// Display jobs on map and in sidebar
function displayJobs(jobs, techLat, techLon) {
    // Clear existing markers
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    
    // Add tech home marker
    const techMarker = L.marker([techLat, techLon], {
        icon: L.divIcon({
            className: 'tech-marker',
            html: '<div style="background:blue;color:white;border-radius:50%;width:30px;height:30px;text-align:center;line-height:30px;">H</div>'
        })
    }).addTo(map);
    techMarker.bindPopup('Tech Home');
    markers.push(techMarker);
    
    // Add job markers
    jobs.forEach(job => {
        if (job.latitude && job.longitude) {
            const color = getJobColor(job.jp_priority);
            const marker = L.marker([job.latitude, job.longitude], {
                icon: L.divIcon({
                    className: 'job-marker',
                    html: `<div style="background:${color};color:white;border-radius:50%;width:25px;height:25px;text-align:center;line-height:25px;">J</div>`
                })
            }).addTo(map);
            
            marker.bindPopup(`
                <strong>${job.site_name}</strong><br>
                ${job.sow_1 || 'No SOW'}<br>
                Due: ${job.due_date}<br>
                Priority: ${job.jp_priority}
            `);
            markers.push(marker);
        }
    });
    
    // Update sidebar
    const listDiv = document.getElementById('job-list');
    listDiv.innerHTML = '';
    
    jobs.forEach(job => {
        const div = document.createElement('div');
        const priorityClass = job.jp_priority.toLowerCase().replace(/\s+/g, '');
        div.className = `job-item ${priorityClass}`;
        div.innerHTML = `
            <strong>${job.site_name}</strong>
            <div class="job-info">
                SOW: ${job.sow_1 || 'None'}<br>
                Due: ${job.due_date}<br>
                Priority: ${job.jp_priority}<br>
                Distance: ${job.distance_miles ? job.distance_miles.toFixed(1) + ' mi' : 'N/A'}
            </div>
        `;
        listDiv.appendChild(div);
    });
    
    // Fit map to show all markers
    if (markers.length > 0) {
        const group = new L.featureGroup(markers);
        map.fitBounds(group.getBounds().pad(0.1));
    }
}

// Generate schedule using Python scheduler
async function generateSchedule() {
    const techId = document.getElementById('tech-select').value;
    const weekStart = document.getElementById('week-start').value;
    
    if (!techId || !weekStart) {
        alert('Please select a technician and week start date');
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/schedule/preview`, {
            method: 'POST',
            headers: {
                'X-API-Key': API_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                tech_id: parseInt(techId),
                start_date: weekStart,
                horizon_days: 21,
                weekly_target_hours_min: 45,
                weekly_target_hours_max: 50
            })
        });
        
        const schedule = await response.json();
        displaySchedule(schedule);
    } catch (error) {
        console.error('Failed to generate schedule:', error);
        alert('Failed to generate schedule. Check console for details.');
    }
}

// Display generated schedule
function displaySchedule(schedule) {
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '';
    
    if (schedule.schedule) {
        schedule.schedule.forEach(day => {
            const dayDiv = document.createElement('div');
            dayDiv.className = 'schedule-day';
            dayDiv.innerHTML = `
                <h4>${day.date} - ${day.total_hours.toFixed(1)} hours</h4>
                ${day.jobs.map(job => `
                    <div class="schedule-job">
                        • ${job.site_name} (${job.sow_1 || 'No SOW'})
                    </div>
                `).join('')}
            `;
            resultsDiv.appendChild(dayDiv);
        });
    }
}

// Helper function for job colors
function getJobColor(priority) {
    switch(priority?.toLowerCase()) {
        case 'nov':
        case 'urgent':
            return '#dc3545';
        case 'monthly o&m':
            return '#ffc107';
        case '3 year':
        case 'annual':
            return '#28a745';
        case 'service':
            return '#17a2b8';
        default:
            return '#6c757d';
    }
}

// Set default week start to next Monday
function setDefaultWeekStart() {
    const today = new Date();
    const day = today.getDay();
    const diff = day === 0 ? 1 : 8 - day; // Days until next Monday
    const nextMonday = new Date(today);
    nextMonday.setDate(today.getDate() + diff);
    
    document.getElementById('week-start').value = nextMonday.toISOString().split('T')[0];
}

// Initialize on page load
window.onload = function() {
    initMap();
    loadTechnicians();
    setDefaultWeekStart();
};
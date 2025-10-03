// JobPool.jsx - Left Panel Component
// ==============================================================================
// COMPONENT EXPLANATION:
// This component displays the list of unscheduled jobs
// It receives data via "props" from the parent (App component)
// ==============================================================================

import React, { useState } from 'react';
import './JobPool.css';

// ==============================================================================
// REACT CONCEPT #5: Props
// Props are like function parameters - they pass data from parent to child
// This component receives: jobs, onJobSelect, selectedTech
// ==============================================================================

function JobPool({ jobs, onJobSelect, selectedTech }) {
  // Local state for filters (only affects this component)
  const [searchTerm, setSearchTerm] = useState('');
  const [regionFilter, setRegionFilter] = useState('');
  const [priorityFilter, setPriorityFilter] = useState('');
  const [urgencyFilter, setUrgencyFilter] = useState('');

  // Filter jobs based on current filters
  const filteredJobs = jobs.filter(job => {
    // Search filter
    if (searchTerm) {
      const searchLower = searchTerm.toLowerCase();
      const searchableText = `${job.work_order} ${job.site_name} ${job.site_city} ${job.site_state}`.toLowerCase();
      if (!searchableText.includes(searchLower)) return false;
    }
    
    // Region filter
    if (regionFilter && job.site_state !== regionFilter) return false;
    
    // Priority filter
    if (priorityFilter && job.jp_priority !== priorityFilter) return false;
    
    // Urgency filter
    if (urgencyFilter && job.urgency !== urgencyFilter) return false;
    
    return true;
  });

  // Helper function to get priority class for styling
  function getPriorityClass(priority) {
    if (priority === 'NOV' || priority === 'Urgent') return 'priority-urgent';
    if (priority === 'Monthly O&M') return 'priority-high';
    return 'priority-normal';
  }

  return (
    <div className="job-pool">
      {/* Panel Header */}
      <div className="panel-header">
        <h2>📋 Job Pool</h2>
        <div className="job-stats">
          <span>{filteredJobs.length} jobs</span>
          <span>{filteredJobs.reduce((sum, j) => sum + (j.est_hours || 2), 0)} hrs</span>
        </div>
      </div>

      {/* Filters */}
      <div className="filters">
        <input
          type="text"
          placeholder="Search jobs..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="filter-input"
        />
        
        <select 
          value={regionFilter} 
          onChange={(e) => setRegionFilter(e.target.value)}
          className="filter-select"
        >
          <option value="">All Regions</option>
          <option value="CO">Colorado</option>
          <option value="UT">Utah</option>
          <option value="AZ">Arizona</option>
        </select>
        
        <select 
          value={priorityFilter} 
          onChange={(e) => setPriorityFilter(e.target.value)}
          className="filter-select"
        >
          <option value="">All Priorities</option>
          <option value="NOV">NOV/Urgent</option>
          <option value="Monthly O&M">Monthly O&M</option>
          <option value="Annual">Annual</option>
        </select>
        
        <select 
          value={urgencyFilter} 
          onChange={(e) => setUrgencyFilter(e.target.value)}
          className="filter-select"
        >
          <option value="">All Urgency</option>
          <option value="critical">Critical (&lt;7 days)</option>
          <option value="high">High (&lt;14 days)</option>
          <option value="normal">Normal</option>
        </select>
      </div>

      {/* Job List */}
      <div className="job-list">
        {filteredJobs.length === 0 ? (
          <div className="empty-state">No jobs match your filters</div>
        ) : (
          filteredJobs.map(job => (
            <div 
              key={job.work_order}
              className="job-card"
              draggable
              onDragStart={(e) => {
                e.dataTransfer.setData('job', JSON.stringify(job));
              }}
            >
              <div className="job-card-header">
                <span className="job-wo">WO {job.work_order}</span>
                <span className={`job-priority ${getPriorityClass(job.jp_priority)}`}>
                  {job.jp_priority || 'N/A'}
                </span>
              </div>
              
              <div className="job-site">{job.site_name}</div>
              <div className="job-location">{job.site_city}, {job.site_state}</div>
              
              <div className="job-meta">
                <span className={`job-urgency urgency-${job.urgency}`}>
                  {job.urgency}
                </span>
                <span>Due: {new Date(job.due_date).toLocaleDateString()}</span>
                <span>{job.est_hours || 2} hrs</span>
              </div>
              
              {selectedTech && (
                <button 
                  className="assign-btn"
                  onClick={() => {
                    const date = prompt('Enter date (YYYY-MM-DD):');
                    if (date) {
                      onJobSelect(job, date);
                    }
                  }}
                >
                  Assign
                </button>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export default JobPool;

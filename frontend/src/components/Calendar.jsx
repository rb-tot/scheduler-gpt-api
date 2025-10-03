// Calendar.jsx - Center Panel (Weekly Calendar)
import React from 'react';
import './Calendar.css';

function Calendar({ schedule, selectedTech, selectedWeek, techs, onTechSelect, onWeekChange, onJobDrop }) {
  // TODO: Full calendar implementation with drag-drop
  // For now, basic structure
  
  return (
    <div className="calendar">
      <div className="panel-header">
        <h2>📅 Schedule</h2>
        <div className="calendar-nav">
          <button onClick={() => onWeekChange('prev')}>◀</button>
          <span>{selectedWeek?.toLocaleDateString()}</span>
          <button onClick={() => onWeekChange('next')}>▶</button>
        </div>
      </div>
      
      <div className="tech-selector">
        <select onChange={(e) => onTechSelect(parseInt(e.target.value))} value={selectedTech || ''}>
          <option value="">Select Tech...</option>
          {techs.map(t => (
            <option key={t.technician_id || t.id} value={t.technician_id || t.id}>
              {t.name}
            </option>
          ))}
        </select>
      </div>
      
      <div className="calendar-grid">
        {selectedTech ? (
          schedule.map(day => (
            <div key={day.date} className="day-column">
              <h4>{day.day_name}</h4>
              <p>{day.total_hours} hrs</p>
              {day.jobs.map(job => (
                <div key={job.work_order} className="scheduled-job">
                  WO {job.work_order}
                </div>
              ))}
            </div>
          ))
        ) : (
          <p>Select a technician</p>
        )}
      </div>
    </div>
  );
}

export default Calendar;

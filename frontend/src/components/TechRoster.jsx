// TechRoster.jsx - Right Panel (Technician List)
import React from 'react';
import './TechRoster.css';

function TechRoster({ techs, selectedTech, onTechSelect }) {
  return (
    <div className="tech-roster">
      <div className="panel-header">
        <h2>👷 Technicians</h2>
      </div>
      
      <div className="tech-list">
        {techs.map(tech => (
          <div 
            key={tech.technician_id || tech.id}
            className={`tech-card ${(tech.technician_id || tech.id) === selectedTech ? 'selected' : ''}`}
            onClick={() => onTechSelect(tech.technician_id || tech.id)}
          >
            <div className="tech-name">{tech.name}</div>
            <div className="tech-capacity">
              <div className="capacity-bar">
                <div 
                  className="capacity-fill"
                  style={{width: `${tech.utilization_percent || 0}%`}}
                />
              </div>
              <span>{tech.current_week_hours || 0}/{tech.max_weekly_hours || 40} hrs</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default TechRoster;

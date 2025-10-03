// SmartSuggestions.jsx - AI Suggestions Panel
import React from 'react';
import './SmartSuggestions.css';

function SmartSuggestions({ suggestions, onSuggestionClick }) {
  if (!suggestions || suggestions.length === 0) return null;
  
  return (
    <div className="smart-suggestions">
      <h3>💡 Smart Suggestions</h3>
      {suggestions.map((sug, idx) => (
        <div key={idx} className="suggestion-card">
          <strong>WO {sug.work_order}</strong>
          <p>{sug.reason}</p>
          <button onClick={() => onSuggestionClick(sug)}>Add</button>
        </div>
      ))}
    </div>
  );
}

export default SmartSuggestions;

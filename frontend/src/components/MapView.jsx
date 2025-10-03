// MapView.jsx - Map Component
import React from 'react';
import './MapView.css';

function MapView({ jobs, techs, selectedTech }) {
  return (
    <div className="map-view">
      <div id="map" style={{width: '100%', height: '100%', background: '#e5e7eb'}}>
        <p style={{textAlign: 'center', paddingTop: '200px'}}>Map integration coming (Leaflet/React-Leaflet)</p>
      </div>
    </div>
  );
}

export default MapView;

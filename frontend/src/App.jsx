// App.jsx - The Main Component
// ==============================================================================
// REACT CONCEPT #1: Components
// A component is like a reusable HTML block with its own logic
// This App component contains your entire scheduler
// ==============================================================================

import React, { useState, useEffect } from 'react';
import './App.css';

// Import our custom components (we'll create these next)
import JobPool from './components/JobPool';
import Calendar from './components/Calendar';
import TechRoster from './components/TechRoster';
import MapView from './components/MapView';
import SmartSuggestions from './components/SmartSuggestions';

// ==============================================================================
// REACT CONCEPT #2: State
// State is data that can change. When state changes, React re-renders the UI
// Think of it as variables that React "watches" for changes
// ==============================================================================

function App() {
  // useState creates a piece of state
  // Syntax: const [value, setValue] = useState(initialValue)
  
  // Jobs state - holds all unscheduled jobs
  const [jobs, setJobs] = useState([]);
  
  // Techs state - holds all technicians
  const [techs, setTechs] = useState([]);
  
  // Selected tech - which tech's calendar are we viewing?
  const [selectedTech, setSelectedTech] = useState(null);
  
  // Selected week - which week are we looking at?
  const [selectedWeek, setSelectedWeek] = useState(getMonday(new Date()));
  
  // Schedule state - the tech's weekly schedule
  const [schedule, setSchedule] = useState([]);
  
  // Suggestions - smart job recommendations
  const [suggestions, setSuggestions] = useState([]);
  
  // Loading state - are we fetching data?
  const [loading, setLoading] = useState(true);

  // ==============================================================================
  // REACT CONCEPT #3: Effects
  // useEffect runs code when component loads or when dependencies change
  // Think of it as "do this when X happens"
  // ==============================================================================
  
  // Load initial data when app starts
  useEffect(() => {
    loadJobs();
    loadTechs();
  }, []); // Empty array = run once when component mounts
  
  // Load schedule when selected tech or week changes
  useEffect(() => {
    if (selectedTech) {
      loadSchedule(selectedTech, selectedWeek);
    }
  }, [selectedTech, selectedWeek]); // Run when these change

  // ==============================================================================
  // API FUNCTIONS - Talk to your backend
  // ==============================================================================
  
  async function loadJobs() {
    try {
      const response = await fetch('/api/jobs/unscheduled');
      const data = await response.json();
      setJobs(data.jobs || []);
    } catch (error) {
      console.error('Failed to load jobs:', error);
    }
  }
  
  async function loadTechs() {
    try {
      const response = await fetch('/api/technicians/all');
      const data = await response.json();
      setTechs(data.technicians || []);
      setLoading(false);
    } catch (error) {
      console.error('Failed to load techs:', error);
      setLoading(false);
    }
  }
  
  async function loadSchedule(techId, weekStart) {
    try {
      const weekStr = formatDate(weekStart);
      const response = await fetch(`/api/schedule/week?tech_id=${techId}&week_start=${weekStr}`);
      const data = await response.json();
      setSchedule(data.days || []);
    } catch (error) {
      console.error('Failed to load schedule:', error);
    }
  }
  
  async function assignJob(workOrder, techId, date, time = '09:00') {
    try {
      const response = await fetch('/api/schedule/assign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          work_order: workOrder,
          technician_id: techId,
          date: date,
          start_time: time
        })
      });
      
      const result = await response.json();
      
      if (result.success) {
        // Refresh data after successful assignment
        await loadJobs();
        await loadTechs();
        if (selectedTech) {
          await loadSchedule(selectedTech, selectedWeek);
        }
        return { success: true };
      } else {
        return { success: false, errors: result.errors };
      }
    } catch (error) {
      console.error('Failed to assign job:', error);
      return { success: false, errors: [error.message] };
    }
  }
  
  async function loadSuggestions(techId, date) {
    try {
      const response = await fetch(`/api/schedule/suggestions?tech_id=${techId}&date=${date}`);
      const data = await response.json();
      setSuggestions(data.suggestions || []);
    } catch (error) {
      console.error('Failed to load suggestions:', error);
    }
  }

  // ==============================================================================
  // EVENT HANDLERS - What happens when user interacts
  // ==============================================================================
  
  function handleTechSelect(techId) {
    setSelectedTech(techId);
    setSelectedWeek(getMonday(new Date())); // Reset to current week
  }
  
  function handleWeekChange(direction) {
    const newWeek = new Date(selectedWeek);
    newWeek.setDate(newWeek.getDate() + (direction === 'next' ? 7 : -7));
    setSelectedWeek(newWeek);
  }
  
  async function handleJobAssign(job, date) {
    if (!selectedTech) {
      alert('Please select a technician first');
      return;
    }
    
    const result = await assignJob(job.work_order, selectedTech, date);
    
    if (result.success) {
      // Load suggestions for that day
      await loadSuggestions(selectedTech, date);
    } else {
      alert('Failed to assign job: ' + result.errors.join(', '));
    }
  }

  // ==============================================================================
  // HELPER FUNCTIONS
  // ==============================================================================
  
  function getMonday(date) {
    const d = new Date(date);
    const day = d.getDay();
    const diff = d.getDate() - day + (day === 0 ? -6 : 1);
    return new Date(d.setDate(diff));
  }
  
  function formatDate(date) {
    return date.toISOString().split('T')[0];
  }

  // ==============================================================================
  // REACT CONCEPT #4: JSX - HTML-like syntax in JavaScript
  // This is what gets rendered to the screen
  // ==============================================================================
  
  if (loading) {
    return (
      <div className="loading-screen">
        <h2>Loading Scheduler...</h2>
      </div>
    );
  }

  return (
    <div className="app">
      {/* Header */}
      <header className="app-header">
        <h1>🗓️ Job Scheduler</h1>
        <div className="header-stats">
          <span>{jobs.length} unscheduled jobs</span>
          <span>{techs.length} active techs</span>
        </div>
      </header>

      {/* Map Section - Full Width */}
      <section className="map-section">
        <MapView 
          jobs={jobs} 
          techs={techs}
          selectedTech={selectedTech}
        />
      </section>

      {/* Main Workspace - 3 Panels */}
      <section className="workspace">
        
        {/* LEFT: Job Pool */}
        <div className="panel panel-jobs">
          <JobPool 
            jobs={jobs}
            onJobSelect={handleJobAssign}
            selectedTech={selectedTech}
          />
        </div>

        {/* CENTER: Calendar */}
        <div className="panel panel-calendar">
          <Calendar 
            schedule={schedule}
            selectedTech={selectedTech}
            selectedWeek={selectedWeek}
            techs={techs}
            onTechSelect={handleTechSelect}
            onWeekChange={handleWeekChange}
            onJobDrop={handleJobAssign}
          />
        </div>

        {/* RIGHT: Tech Roster + Suggestions */}
        <div className="panel panel-techs">
          <TechRoster 
            techs={techs}
            selectedTech={selectedTech}
            onTechSelect={handleTechSelect}
          />
          
          {suggestions.length > 0 && (
            <SmartSuggestions 
              suggestions={suggestions}
              onSuggestionClick={handleJobAssign}
            />
          )}
        </div>

      </section>
    </div>
  );
}

export default App;

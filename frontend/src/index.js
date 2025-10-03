// index.js - This is where React starts
// Think of this as the "main()" function in other languages

import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';

// This line tells React to render your App component into the HTML
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

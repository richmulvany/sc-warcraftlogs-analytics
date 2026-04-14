import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import { ColourBlindProvider } from './context/ColourBlindContext'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ColourBlindProvider>
      <App />
    </ColourBlindProvider>
  </React.StrictMode>
)

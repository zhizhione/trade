import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import { HistoricalReplay } from './HistoricalReplay';
import './styles.css';

const replay = new URLSearchParams(window.location.search).get('view') === 'replay';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {replay ? <HistoricalReplay /> : <App />}
  </StrictMode>,
);

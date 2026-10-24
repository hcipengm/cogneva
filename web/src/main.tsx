import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import { bootTheme } from '@/styles/themes';
import './styles/index.css';

// 先套用上次选的主题，再渲染：晚一步就会先画出默认配色、再跳成选中的那一套。
bootTheme();

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element #root not found');
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>
);

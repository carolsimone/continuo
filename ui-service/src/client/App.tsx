import { BrowserRouter, Routes, Route } from 'react-router-dom';
import DashboardPage from './DashboardPage';
import DetailPage from './DetailPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/schedule/:name" element={<DetailPage />} />
      </Routes>
    </BrowserRouter>
  );
}

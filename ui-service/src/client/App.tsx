import { BrowserRouter, Routes, Route } from 'react-router-dom';
import DashboardPage from './DashboardPage';
import DetailPage from './DetailPage';
import NodeDetailPage from './NodeDetailPage';
import ReleaseDetailPage from './ReleaseDetailPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/schedule/:name" element={<DetailPage />} />
        <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
        <Route path="/schedule/:name/node/:fqn" element={<NodeDetailPage />} />
        <Route path="/releases/:id" element={<ReleaseDetailPage />} />
      </Routes>
    </BrowserRouter>
  );
}

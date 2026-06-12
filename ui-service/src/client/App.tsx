import { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import DashboardPage from './DashboardPage';
import DetailPage from './DetailPage';
import NodeDetailPage from './NodeDetailPage';
import ReleaseDetailPage from './ReleaseDetailPage';
import ChatContainer from './chat/ChatContainer';

export default function App() {
  const [chatEnabled, setChatEnabled] = useState(false);

  // The chat bridge is attached server-side only when CHAT_BRIDGE_ENABLED is set.
  // Read the flag before mounting the panel so production (bridge off by default)
  // never opens a socket the server isn't serving.
  useEffect(() => {
    fetch('/api/features')
      .then((r) => (r.ok ? r.json() : { chatBridgeEnabled: false }))
      .then((d) => setChatEnabled(Boolean(d.chatBridgeEnabled)))
      .catch(() => setChatEnabled(false));
  }, []);

  return (
    <BrowserRouter>
      <div className="app-shell">
        <div className="app-shell__main">
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/schedule/:name" element={<DetailPage />} />
            <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
            <Route path="/node/:fqn" element={<NodeDetailPage />} />
            <Route path="/releases/:id" element={<ReleaseDetailPage />} />
          </Routes>
        </div>
        {chatEnabled && <ChatContainer />}
      </div>
    </BrowserRouter>
  );
}

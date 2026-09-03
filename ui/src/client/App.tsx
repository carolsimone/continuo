import { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router';
import DashboardPage from './DashboardPage';
import DetailPage from './DetailPage';
import NodeDetailPage from './NodeDetailPage';
import ReleaseDetailPage from './ReleaseDetailPage';
import VerificationDetailPage from './VerificationDetailPage';
import ChatContainer from './chat/ChatContainer';
import { useAuth } from './auth/useAuth';
import { AuthContext } from './auth/AuthContext';
import SignInPage from './auth/SignInPage';

export default function App() {
  const auth = useAuth();
  const [chatEnabled, setChatEnabled] = useState(false);

  // The chat bridge is attached server-side only when CHAT_BRIDGE_ENABLED is
  // set; read the flag after authentication so production (bridge off, or a
  // viewer-role user) never opens a socket the server would refuse.
  useEffect(() => {
    if (auth.status !== 'authenticated') return;
    fetch('/api/features')
      .then((r) => (r.ok ? r.json() : { chatBridgeEnabled: false }))
      .then((d) => setChatEnabled(Boolean(d.chatBridgeEnabled)))
      .catch(() => setChatEnabled(false));
  }, [auth.status]);

  if (auth.status === 'loading') return null;
  if (auth.status === 'unauthenticated') return <SignInPage />;

  return (
    <AuthContext.Provider value={auth.user}>
      <BrowserRouter>
        <div className="app-shell">
          <div className="app-shell__main">
            <Routes>
              <Route path="/" element={<DashboardPage />} />
              <Route path="/schedule/:name" element={<DetailPage />} />
              <Route path="/schedule/:name/latest" element={<DetailPage mode="latest" />} />
              <Route path="/node/:fqn" element={<NodeDetailPage />} />
              <Route path="/releases/:id" element={<ReleaseDetailPage />} />
              <Route path="/verifications/:id" element={<VerificationDetailPage />} />
            </Routes>
          </div>
          {chatEnabled && auth.user.role === 'operator' && <ChatContainer />}
        </div>
      </BrowserRouter>
    </AuthContext.Provider>
  );
}

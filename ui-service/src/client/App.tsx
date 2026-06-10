import { BrowserRouter, Routes, Route } from 'react-router-dom';
import DashboardPage from './DashboardPage';
import DetailPage from './DetailPage';
import NodeDetailPage from './NodeDetailPage';
import ReleaseDetailPage from './ReleaseDetailPage';
import ChatPanel from './ChatPanel';
import { useChatSocket } from './chat/useChatSocket';

export default function App() {
  const chat = useChatSocket();
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
        <ChatPanel
          items={chat.state.items}
          connected={chat.connected}
          onSend={chat.send}
          onNewChat={chat.newChat}
        />
      </div>
    </BrowserRouter>
  );
}

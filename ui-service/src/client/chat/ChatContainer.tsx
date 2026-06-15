import ChatPanel from '../ChatPanel';
import { useChatSocket } from './useChatSocket';

// Wraps the chat hook and panel so they mount only when the server bridge is
// enabled. Keeping useChatSocket here (rather than at the App root) means the
// WebSocket is never opened when the bridge is off, avoiding an endlessly
// reconnecting, permanently disconnected panel in production.
export default function ChatContainer() {
  const chat = useChatSocket();
  return (
    <ChatPanel
      items={chat.state.items}
      connected={chat.connected}
      pendingConfirm={chat.state.pendingConfirm}
      streaming={chat.state.streaming}
      onSend={chat.send}
      onNewChat={chat.newChat}
      onConfirm={chat.confirm}
      onInterrupt={chat.interrupt}
    />
  );
}

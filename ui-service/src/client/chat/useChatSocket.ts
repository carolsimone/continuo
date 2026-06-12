import { useCallback, useEffect, useRef, useState } from 'react';
import type { ServerMessage } from './chat-protocol';
import { ChatState, applyServerMessage, appendUserText, initialChatState } from './chat-state';

const SESSION_KEY = 'continuo.chat.session';

export interface ChatSocket {
  state: ChatState;
  connected: boolean;
  send: (text: string) => void;
  newChat: () => void;
}

export function useChatSocket(): ChatSocket {
  const [state, setState] = useState<ChatState>(() => ({
    ...initialChatState,
    sessionId: sessionStorage.getItem(SESSION_KEY),
  }));
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let unmounted = false;
    let retry = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let ws: WebSocket;

    function connect() {
      const stored = sessionStorage.getItem(SESSION_KEY);
      const qs = stored ? `?sessionId=${encodeURIComponent(stored)}` : '';
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${window.location.host}/ws/chat${qs}`);
      wsRef.current = ws;
      ws.onopen = () => {
        retry = 0;
        setConnected(true);
      };
      ws.onclose = () => {
        setConnected(false);
        if (unmounted) return;
        const delay = Math.min(1000 * 2 ** retry, 15000);
        retry += 1;
        timer = setTimeout(connect, delay);
      };
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data) as ServerMessage;
        if (msg.type === 'session') sessionStorage.setItem(SESSION_KEY, msg.sessionId);
        setState((s) => applyServerMessage(s, msg));
      };
    }

    connect();
    return () => {
      unmounted = true;
      if (timer) clearTimeout(timer);
      ws.close();
    };
  }, []);

  const send = useCallback((text: string) => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    setState((s) => appendUserText(s, text));
    ws.send(JSON.stringify({ type: 'user_message', text }));
  }, []);

  const newChat = useCallback(() => {
    const ws = wsRef.current;
    sessionStorage.removeItem(SESSION_KEY);
    setState({ ...initialChatState, sessionId: null });
    if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'new_chat' }));
  }, []);

  return { state, connected, send, newChat };
}

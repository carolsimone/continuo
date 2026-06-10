import { useState } from 'react';
import type { FormEvent } from 'react';
import ReactMarkdown from 'react-markdown';
import type { ChatItem } from './chat/chat-state';

export interface ChatPanelProps {
  items: ChatItem[];
  connected: boolean;
  onSend: (text: string) => void;
  onNewChat: () => void;
}

export default function ChatPanel({ items, connected, onSend, onNewChat }: ChatPanelProps) {
  const [draft, setDraft] = useState('');

  function submit(e: FormEvent) {
    e.preventDefault();
    const text = draft.trim();
    if (!text) return;
    onSend(text);
    setDraft('');
  }

  return (
    <aside className="chat-panel" aria-label="Continuo assistant">
      <header className="chat-panel__header">
        <span className="chat-panel__title">Assistant</span>
        <button type="button" className="chat-panel__new" onClick={onNewChat}>
          New chat
        </button>
      </header>
      <div className="chat-panel__messages">
        {items.map((item, i) => {
          if (item.kind === 'user') {
            return (
              <div key={i} className="chat-msg chat-msg--user">
                {item.text}
              </div>
            );
          }
          if (item.kind === 'tool') {
            return (
              <div key={i} className="chat-msg chat-msg--tool">
                running <code>{item.command}</code>…
              </div>
            );
          }
          if (item.kind === 'error') {
            return (
              <div key={i} className="chat-msg chat-msg--error">
                {item.message}
              </div>
            );
          }
          return (
            <div key={i} className="chat-msg chat-msg--assistant">
              <ReactMarkdown>{item.text}</ReactMarkdown>
            </div>
          );
        })}
      </div>
      <form className="chat-panel__input" onSubmit={submit}>
        <input
          aria-label="Message"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="Ask about your schedules…"
        />
        <button type="submit" disabled={!connected}>
          Send
        </button>
      </form>
    </aside>
  );
}

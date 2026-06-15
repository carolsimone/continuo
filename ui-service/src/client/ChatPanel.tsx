import { useState } from 'react';
import type { FormEvent } from 'react';
import ReactMarkdown from 'react-markdown';
import type { ChatItem } from './chat/chat-state';

export interface ChatPanelProps {
  items: ChatItem[];
  connected: boolean;
  pendingConfirm: string | null;
  streaming: boolean;
  onSend: (text: string) => void;
  onNewChat: () => void;
  onConfirm: (actionId: string, approved: boolean) => void;
  onInterrupt: () => void;
}

export default function ChatPanel({ items, connected, pendingConfirm, streaming, onSend, onNewChat, onConfirm, onInterrupt }: ChatPanelProps) {
  const [draft, setDraft] = useState('');

  function submit(e: FormEvent) {
    e.preventDefault();
    const text = draft.trim();
    if (!text) return;
    onSend(text);
    setDraft('');
  }

  const inputDisabled = !connected || pendingConfirm !== null;

  return (
    <aside className="chat-panel" aria-label="Continuo assistant">
      <header className="chat-panel__header">
        <span className="chat-panel__title">Assistant</span>
        {streaming && (
          <button type="button" className="chat-panel__stop" onClick={onInterrupt}>
            Stop
          </button>
        )}
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
          if (item.kind === 'confirm') {
            return (
              <div key={i} className="chat-msg chat-msg--confirm">
                <span>{item.summary}</span>
                {item.resolved === null ? (
                  <span className="chat-msg__confirm-actions">
                    <button type="button" onClick={() => onConfirm(item.actionId, true)}>
                      Confirm
                    </button>
                    <button type="button" onClick={() => onConfirm(item.actionId, false)}>
                      Deny
                    </button>
                  </span>
                ) : (
                  <em>{item.resolved ? 'confirmed' : 'denied'}</em>
                )}
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
          disabled={inputDisabled}
        />
        <button type="submit" disabled={inputDisabled}>
          Send
        </button>
      </form>
    </aside>
  );
}

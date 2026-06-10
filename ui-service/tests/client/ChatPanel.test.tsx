// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ChatPanel from '../../src/client/ChatPanel';
import type { ChatItem } from '../../src/client/chat/chat-state';

describe('ChatPanel', () => {
  it('renders user, tool, assistant (markdown), and error items', () => {
    const items: ChatItem[] = [
      { kind: 'user', text: 'how is daily?' },
      { kind: 'tool', command: 'continuo schedule status daily' },
      { kind: 'assistant', text: '**All** green', done: true },
      { kind: 'error', message: 'agent failed' },
    ];
    render(<ChatPanel items={items} connected onSend={() => {}} onNewChat={() => {}} />);
    expect(screen.getByText('how is daily?')).toBeInTheDocument();
    expect(screen.getByText('continuo schedule status daily')).toBeInTheDocument();
    expect(screen.getByText('All').tagName).toBe('STRONG');
    expect(screen.getByText('agent failed')).toBeInTheDocument();
  });

  it('sends the trimmed draft and clears the input', async () => {
    const onSend = vi.fn();
    render(<ChatPanel items={[]} connected onSend={onSend} onNewChat={() => {}} />);
    const input = screen.getByLabelText('Message') as HTMLInputElement;
    await userEvent.type(input, '  hello  ');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    expect(onSend).toHaveBeenCalledWith('hello');
    expect(input.value).toBe('');
  });

  it('keeps the input enabled so follow-ups can be queued while answering', () => {
    render(<ChatPanel items={[]} connected onSend={() => {}} onNewChat={() => {}} />);
    expect(screen.getByLabelText('Message')).toBeEnabled();
  });

  it('calls onNewChat when the New chat button is clicked', async () => {
    const onNewChat = vi.fn();
    render(<ChatPanel items={[]} connected onSend={() => {}} onNewChat={onNewChat} />);
    await userEvent.click(screen.getByRole('button', { name: 'New chat' }));
    expect(onNewChat).toHaveBeenCalledOnce();
  });
});

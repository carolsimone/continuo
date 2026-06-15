// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import ChatPanel from '../../src/client/ChatPanel';
import type { ChatItem } from '../../src/client/chat/chat-state';

const noopConfirm = () => {};
const noopInterrupt = () => {};

describe('ChatPanel', () => {
  it('renders user, tool, assistant (markdown), and error items', () => {
    const items: ChatItem[] = [
      { kind: 'user', text: 'how is daily?' },
      { kind: 'tool', command: 'continuo schedule status daily' },
      { kind: 'assistant', text: '**All** green', done: true },
      { kind: 'error', message: 'agent failed' },
    ];
    render(
      <ChatPanel
        items={items}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.getByText('how is daily?')).toBeInTheDocument();
    expect(screen.getByText('continuo schedule status daily')).toBeInTheDocument();
    expect(screen.getByText('All').tagName).toBe('STRONG');
    expect(screen.getByText('agent failed')).toBeInTheDocument();
  });

  it('sends the trimmed draft and clears the input', async () => {
    const onSend = vi.fn();
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={onSend}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    const input = screen.getByLabelText('Message') as HTMLInputElement;
    await userEvent.type(input, '  hello  ');
    await userEvent.click(screen.getByRole('button', { name: 'Send' }));
    expect(onSend).toHaveBeenCalledWith('hello');
    expect(input.value).toBe('');
  });

  it('keeps the input enabled when connected and no pending confirm', () => {
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.getByLabelText('Message')).toBeEnabled();
  });

  it('disables input while a confirm is pending', () => {
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm="a1"
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.getByLabelText('Message')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Send' })).toBeDisabled();
  });

  it('calls onNewChat when the New chat button is clicked', async () => {
    const onNewChat = vi.fn();
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={() => {}}
        onNewChat={onNewChat}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    await userEvent.click(screen.getByRole('button', { name: 'New chat' }));
    expect(onNewChat).toHaveBeenCalledOnce();
  });

  it('shows Stop button while streaming and calls onInterrupt', async () => {
    const onInterrupt = vi.fn();
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm={null}
        streaming={true}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={onInterrupt}
      />,
    );
    const stopBtn = screen.getByRole('button', { name: 'Stop' });
    expect(stopBtn).toBeInTheDocument();
    await userEvent.click(stopBtn);
    expect(onInterrupt).toHaveBeenCalledOnce();
  });

  it('does not show Stop button when not streaming', () => {
    render(
      <ChatPanel
        items={[]}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.queryByRole('button', { name: 'Stop' })).not.toBeInTheDocument();
  });

  it('renders confirm item with Confirm and Deny buttons when unresolved', async () => {
    const onConfirm = vi.fn();
    const items: ChatItem[] = [
      { kind: 'confirm', actionId: 'a1', summary: 'Run the daily schedule?', resolved: null },
    ];
    render(
      <ChatPanel
        items={items}
        connected
        pendingConfirm="a1"
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={onConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.getByText('Run the daily schedule?')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Confirm' }));
    expect(onConfirm).toHaveBeenCalledWith('a1', true);
  });

  it('renders resolved confirm item as italic text', () => {
    const items: ChatItem[] = [
      { kind: 'confirm', actionId: 'a1', summary: 'Run the daily schedule?', resolved: true },
    ];
    render(
      <ChatPanel
        items={items}
        connected
        pendingConfirm={null}
        streaming={false}
        onSend={() => {}}
        onNewChat={() => {}}
        onConfirm={noopConfirm}
        onInterrupt={noopInterrupt}
      />,
    );
    expect(screen.getByText('confirmed')).toBeInTheDocument();
  });
});

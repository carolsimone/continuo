// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ErrorCell from './ErrorCell';

describe('ErrorCell', () => {
  it('gives a 40-character message a title and a more toggle', () => {
    const message = 'no remediation type matches this failure';
    expect(message).toHaveLength(40);
    const { container } = render(<ErrorCell message={message} />);

    const short = container.querySelector('.nodes-error-short') as HTMLElement;
    expect(short).toHaveAttribute('title', message);
    expect(short.textContent).toBe(message);

    fireEvent.click(screen.getByRole('button', { name: 'more' }));
    expect(short).toHaveClass('nodes-error-short--hidden');
    expect(container.querySelector('.nodes-error-full')).toHaveClass('nodes-error-full--visible');
    expect(screen.getByRole('button', { name: 'less' })).toBeInTheDocument();
  });

  it('shows a short message whole, with no toggle', () => {
    const { container } = render(<ErrorCell message="duplicate column" />);
    expect(container.querySelector('.nodes-error-short')?.textContent).toBe('duplicate column');
    expect(screen.queryByRole('button', { name: 'more' })).toBeNull();
  });

  it('offers the toggle from the first character the clipped line cannot show', () => {
    const fits = 'x'.repeat(27);
    const { unmount } = render(<ErrorCell message={fits} />);
    expect(screen.queryByRole('button', { name: 'more' })).toBeNull();
    unmount();

    render(<ErrorCell message={'x'.repeat(28)} />);
    expect(screen.getByRole('button', { name: 'more' })).toBeInTheDocument();
  });
});

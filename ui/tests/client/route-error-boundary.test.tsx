// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router';
import RouteErrorBoundary from '../../src/client/RouteErrorBoundary';

function Boom(): never {
  throw new Error('boom');
}

function ToSafePage() {
  const navigate = useNavigate();
  return <button onClick={() => navigate('/safe')}>to safe</button>;
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <ToSafePage />
      <RouteErrorBoundary>
        <Routes>
          <Route path="/boom" element={<Boom />} />
          <Route path="/safe" element={<p>safe page</p>} />
        </Routes>
      </RouteErrorBoundary>
    </MemoryRouter>,
  );
}

describe('RouteErrorBoundary', () => {
  // React logs every caught render error to the console.
  beforeEach(() => { vi.spyOn(console, 'error').mockImplementation(() => {}); });
  afterEach(() => { vi.restoreAllMocks(); });

  it('renders its children when nothing throws', () => {
    renderAt('/safe');
    expect(screen.getByText('safe page')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Reload' })).toBeNull();
  });

  it('shows a message and a reload button when a route throws while rendering', () => {
    renderAt('/boom');
    expect(screen.getByText('This page failed to display.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload' })).toBeInTheDocument();
  });

  it('shows the next page when the user navigates away from the failed one', () => {
    renderAt('/boom');
    fireEvent.click(screen.getByRole('button', { name: 'to safe' }));
    expect(screen.getByText('safe page')).toBeInTheDocument();
    expect(screen.queryByText('This page failed to display.')).toBeNull();
  });
});

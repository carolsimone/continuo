// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import SnapshotTile from './SnapshotTile';

function renderAt(summary: any) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<SnapshotTile summary={summary} now={new Date('2026-05-26T12:05:00Z')} />} />
        <Route path="/schedule/:name/latest" element={<div>LATEST</div>} />
      </Routes>
    </MemoryRouter>
  );
}

describe('SnapshotTile', () => {
  it('renders name, node count and relative time', () => {
    renderAt({ schedule_name: 'e2e', node_count: 13, last_updated_at: '2026-05-26T12:00:00Z' });
    expect(screen.getByText('e2e')).toBeInTheDocument();
    expect(screen.getByText(/13 nodes/)).toBeInTheDocument();
    expect(screen.getByText(/5m ago/)).toBeInTheDocument();
  });

  it('navigates to /schedule/:name/latest on click', () => {
    renderAt({ schedule_name: 'e2e', node_count: 13, last_updated_at: '2026-05-26T12:00:00Z' });
    fireEvent.click(screen.getByRole('button', { name: /e2e/i }));
    expect(screen.getByText('LATEST')).toBeInTheDocument();
  });
});

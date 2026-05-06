// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { useState } from 'react';
import RerunConfirmDialog from '../../src/client/RerunConfirmDialog';

describe('RerunConfirmDialog — stale state', () => {
  function renderStale(onConfirm = vi.fn(), onCancel = vi.fn()) {
    render(
      <RerunConfirmDialog
        state="stale"
        runGen={5}
        latestGen={7}
        onConfirm={onConfirm}
        onCancel={onCancel}
      />,
    );
    return { onConfirm, onCancel };
  }

  it('renders title, chip, body, and CTA', () => {
    renderStale();
    expect(screen.getByText('Stale topology')).toBeInTheDocument();
    expect(screen.getByText('5 → 7')).toBeInTheDocument();
    expect(
      screen.getByText(/re-ingested 2 times since this run started/),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Rerun with old snapshot' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument();
  });

  it('calls onConfirm when CTA is clicked', () => {
    const { onConfirm, onCancel } = renderStale();
    fireEvent.click(screen.getByRole('button', { name: 'Rerun with old snapshot' }));
    expect(onConfirm).toHaveBeenCalledTimes(1);
    expect(onCancel).not.toHaveBeenCalled();
  });

  it('calls onCancel when Cancel button is clicked', () => {
    const { onConfirm, onCancel } = renderStale();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it('calls onCancel when Escape is pressed', () => {
    const { onConfirm, onCancel } = renderStale();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it('calls onCancel when overlay backdrop is clicked', () => {
    const { onConfirm, onCancel } = renderStale();
    const overlay = screen.getByTestId('rerun-confirm-overlay');
    fireEvent.click(overlay);
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it('does NOT call onCancel when the dialog body is clicked', () => {
    const { onCancel } = renderStale();
    fireEvent.click(screen.getByRole('dialog'));
    expect(onCancel).not.toHaveBeenCalled();
  });
});

describe('RerunConfirmDialog — unknown state', () => {
  it('renders the unknown copy', () => {
    render(
      <RerunConfirmDialog
        state="unknown"
        runGen={0}
        latestGen={7}
        onConfirm={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    expect(screen.getByText('Topology unknown')).toBeInTheDocument();
    expect(screen.getByText('no version recorded')).toBeInTheDocument();
    expect(
      screen.getByText(/This run started before topology tracking existed/),
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Rerun anyway' })).toBeInTheDocument();
  });
});

describe('RerunConfirmDialog — modal-freeze behaviour', () => {
  it('keeps showing the gen values captured at mount time even if props change', () => {
    function Harness() {
      const [latest, setLatest] = useState(7);
      return (
        <>
          <button data-testid="bump" onClick={() => setLatest(8)}>bump</button>
          <RerunConfirmDialog
            state="stale"
            runGen={5}
            latestGen={latest}
            onConfirm={vi.fn()}
            onCancel={vi.fn()}
          />
        </>
      );
    }
    render(<Harness />);
    expect(screen.getByText('5 → 7')).toBeInTheDocument();
    fireEvent.click(screen.getByTestId('bump'));
    expect(screen.getByText('5 → 7')).toBeInTheDocument();
    expect(screen.queryByText('5 → 8')).toBeNull();
  });
});

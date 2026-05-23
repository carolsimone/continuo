// @vitest-environment jsdom
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import RerunFailedModal from '../../src/client/RerunFailedModal';

describe('RerunFailedModal', () => {
  it('renders both radio choices with their descriptions', () => {
    render(
      <RerunFailedModal
        driftLabel={null}
        submitting={false}
        onClose={() => {}}
        onSubmit={() => {}}
      />,
    );
    expect(screen.getByLabelText(/this snapshot/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/latest snapshot/i)).toBeInTheDocument();
    expect(screen.getByText(/reuse the image \+ manifest pinned/i)).toBeInTheDocument();
    expect(screen.getByText(/use current image \+ manifest/i)).toBeInTheDocument();
  });

  it('shows the drift info-strip only when driftLabel is set', () => {
    const { rerender, container } = render(
      <RerunFailedModal
        driftLabel={null}
        submitting={false}
        onClose={() => {}}
        onSubmit={() => {}}
      />,
    );
    expect(container.querySelector('.info-strip--warning')).toBeNull();

    rerender(
      <RerunFailedModal
        driftLabel="source 1 gen behind latest"
        submitting={false}
        onClose={() => {}}
        onSubmit={() => {}}
      />,
    );
    const strip = screen.getByText('source 1 gen behind latest');
    expect(strip.closest('.info-strip--warning')).toBeInTheDocument();
  });

  it('Cancel and Rerun use the .btn foundation', () => {
    render(
      <RerunFailedModal
        driftLabel={null}
        submitting={false}
        onClose={() => {}}
        onSubmit={() => {}}
      />,
    );
    const cancel = screen.getByRole('button', { name: /cancel/i });
    const rerun = screen.getByRole('button', { name: /^rerun$/i });
    expect(cancel.className).toMatch(/\bbtn--secondary\b/);
    expect(rerun.className).toMatch(/\bbtn--primary\b/);
  });

  it('Rerun fires onSubmit with the selected mode', async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(
      <RerunFailedModal
        driftLabel={null}
        submitting={false}
        onClose={() => {}}
        onSubmit={onSubmit}
      />,
    );

    // Default selection is "this snapshot" — first radio.
    await user.click(screen.getByRole('button', { name: /^rerun$/i }));
    expect(onSubmit).toHaveBeenLastCalledWith('this');

    await user.click(screen.getByLabelText(/latest snapshot/i));
    await user.click(screen.getByRole('button', { name: /^rerun$/i }));
    expect(onSubmit).toHaveBeenLastCalledWith('latest');
  });

  it('Cancel fires onClose', async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <RerunFailedModal
        driftLabel={null}
        submitting={false}
        onClose={onClose}
        onSubmit={() => {}}
      />,
    );
    await user.click(screen.getByRole('button', { name: /cancel/i }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('disables both buttons while submitting', () => {
    render(
      <RerunFailedModal
        driftLabel={null}
        submitting={true}
        onClose={() => {}}
        onSubmit={() => {}}
      />,
    );
    expect(screen.getByRole('button', { name: /cancel/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /^rerun$/i })).toBeDisabled();
  });
});

// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import MaintenanceBanner from './MaintenanceBanner';

describe('MaintenanceBanner', () => {
  it('says continuo is in maintenance mode', () => {
    render(<MaintenanceBanner />);
    expect(screen.getByRole('status').textContent).toMatch(/continuo is in maintenance mode/);
  });
});

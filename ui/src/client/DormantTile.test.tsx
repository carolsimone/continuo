// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router';
import DormantTile from './DormantTile';

// Shows where the tile navigated to.
function LocationProbe() {
  const location = useLocation();
  return <div data-testid="probe">{`${location.pathname}${location.search}`}</div>;
}

function renderAt(count: number) {
  return render(
    <MemoryRouter initialEntries={['/?tab=topology']}>
      <DormantTile count={count} />
      <LocationProbe />
      <Routes>
        <Route path="/" element={null} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('DormantTile', () => {
  it('names the dormant nodes and says they run by hand', () => {
    renderAt(4);
    expect(screen.getByText('Dormant')).toBeInTheDocument();
    expect(screen.getByText('4 dormant nodes · run by hand')).toBeInTheDocument();
  });

  it('counts a single dormant node in the singular', () => {
    renderAt(1);
    expect(screen.getByText('1 dormant node · run by hand')).toBeInTheDocument();
  });

  it('opens the Nodes tab filtered to dormant nodes on click', () => {
    renderAt(4);
    fireEvent.click(screen.getByRole('button', { name: /dormant/i }));
    expect(screen.getByTestId('probe')).toHaveTextContent('/?tab=nodes&dormant=1');
  });
});

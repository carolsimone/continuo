// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import PageHeader from '../../src/client/PageHeader';
import { AuthContext } from '../../src/client/auth/AuthContext';

const USER = { userId: 'u1', email: 'op@example.com', name: 'Op', role: 'operator' as const };

describe('PageHeader', () => {
  it('starts with the brand, then a divider, then the page identity', () => {
    const { container } = render(
      <PageHeader>
        <button className="detail-back-link">← Back</button>
        <div className="detail-page-title">rel-1</div>
      </PageHeader>,
    );
    const header = container.querySelector('header.page-header')!;
    const children = Array.from(header.children);
    expect(children[0].matches('a.brand')).toBe(true);
    expect(children[0]).toHaveAttribute('href', '/');
    expect(children[1].matches('.page-header__divider')).toBe(true);
    expect(children[2].matches('.detail-back-link')).toBe(true);
    expect(children[3].matches('.detail-page-title')).toBe(true);
  });

  it('omits the divider when the page has no identity of its own', () => {
    const { container } = render(<PageHeader />);
    expect(container.querySelector('.page-header a.brand')).toBeTruthy();
    expect(container.querySelector('.page-header__divider')).toBeNull();
  });

  it('wraps the brand in the page heading when the brand is the title', () => {
    const { container } = render(<PageHeader brandAsTitle />);
    expect(container.querySelector('.page-header h1 a.brand')).toBeTruthy();
    expect(container.querySelector('.page-header > a.brand')).toBeNull();
  });

  it('right-aligns the actions slot followed by the user menu of the signed-in user', () => {
    const { container } = render(
      <AuthContext.Provider value={USER}>
        <PageHeader actions={<span className="live-badge">● Live</span>} />
      </AuthContext.Provider>,
    );
    const actions = container.querySelector('.page-header > .page-actions')!;
    expect(actions).toBeTruthy();
    const kids = Array.from(actions.children);
    expect(kids[0].matches('.live-badge')).toBe(true);
    expect(kids[1].matches('.user-menu')).toBe(true);
    expect(kids[1].textContent).toContain('op@example.com');
  });

  it('renders no user menu when nobody is signed in', () => {
    const { container } = render(<PageHeader />);
    expect(container.querySelector('.user-menu')).toBeNull();
  });
});

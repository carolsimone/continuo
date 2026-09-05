import { ReactNode } from 'react';
import Brand from './Brand';
import UserMenu from './auth/UserMenu';

// PageHeader is the identity row every page renders: the product brand first,
// always in the same place, then the page's own identity (back link, title,
// status pill, context chips) after a hairline divider, and a right-aligned
// actions slot that always ends with the signed-in user's menu. The home page
// has no identity of its own — the brand is its title — so it passes no
// children and wraps the brand in the page heading instead.
type Props = {
  brandAsTitle?: boolean;
  actions?: ReactNode;
  children?: ReactNode;
};

export default function PageHeader({ brandAsTitle = false, actions, children }: Props) {
  const hasIdentity = children !== undefined && children !== null && children !== false;
  return (
    <header className="page-header">
      {brandAsTitle ? <h1><Brand /></h1> : <Brand />}
      {hasIdentity && <span className="page-header__divider" aria-hidden="true" />}
      {children}
      <div className="page-actions">
        {actions}
        <UserMenu />
      </div>
    </header>
  );
}

import { Component, type ReactNode } from 'react';
import { useLocation } from 'react-router';

interface BoundaryProps {
  resetKey: string;
  children: ReactNode;
}

class Boundary extends Component<BoundaryProps, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidUpdate(previous: BoundaryProps) {
    if (this.state.failed && previous.resetKey !== this.props.resetKey) {
      this.setState({ failed: false });
    }
  }

  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <div className="page">
        <main className="page-content">
          <div className="graph-empty-state">
            <p className="graph-empty-title">This page failed to display.</p>
            <p className="graph-empty-copy">Reload to try again.</p>
            <button type="button" className="btn btn--secondary" onClick={() => window.location.reload()}>
              Reload
            </button>
          </div>
        </main>
      </div>
    );
  }
}

// Catches an error thrown while a route renders, so one page's failure shows a
// message inside the app shell rather than unmounting the whole app. Navigating
// to another path clears it.
export default function RouteErrorBoundary({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  return <Boundary resetKey={pathname}>{children}</Boundary>;
}

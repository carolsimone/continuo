import '@testing-library/jest-dom/vitest';
import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

// jsdom doesn't ship ResizeObserver; @xyflow/react and other libraries
// instantiate one during render. Provide a no-op stub so DOM tests can mount
// components that depend on it.
if (typeof globalThis.ResizeObserver === 'undefined') {
  class ResizeObserverStub {
    observe(): void {}
    unobserve(): void {}
    disconnect(): void {}
  }
  globalThis.ResizeObserver = ResizeObserverStub as typeof ResizeObserver;
}

// jsdom doesn't ship scrollIntoView; the Nodes panel calls it after a
// node selection.
if (typeof Element !== 'undefined' && !Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = function () {};
}

afterEach(() => {
  cleanup();
});

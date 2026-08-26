// @vitest-environment jsdom
import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import Brand from '../../src/client/Brand';

describe('Brand', () => {
  it('renders the mark next to the wordmark', () => {
    const { container } = render(<Brand />);
    const root = container.querySelector('.brand');
    expect(root).toBeInTheDocument();
    expect(root).toHaveTextContent('Continuo');

    // The mark is decorative: the wordmark carries the name, so the image
    // must not be announced a second time by screen readers.
    const mark = container.querySelector('img.brand__mark');
    expect(mark).toHaveAttribute('src', '/mark.svg');
    expect(mark).toHaveAttribute('alt', '');
    expect(mark).toHaveAttribute('width', '24');
    expect(mark).toHaveAttribute('height', '24');
  });
});

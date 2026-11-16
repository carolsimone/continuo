import { useState } from 'react';

// The characters one line of `.nodes-error-short` shows before it clips: the
// line is at most 180px wide and its 11px monospace font draws each character
// about 0.6em (6.6px) wide, so 180 / 6.6 = 27.27 characters fit.
const CHARS_PER_LINE = 27;

// ErrorCell shows an error text on one truncated line, with the whole text as
// the line's title and a `more` toggle that expands it in place (and `less`
// that folds it back). A text short enough to fit the line has no toggle. The
// click and key events of the toggle stop here, so a row or group header that
// handles them is not triggered.
export default function ErrorCell({ message }: { message: string }) {
  const [expanded, setExpanded] = useState(false);
  const isLong = message.length > CHARS_PER_LINE;
  return (
    <div className="nodes-error-text">
      <span className={`nodes-error-short${expanded ? ' nodes-error-short--hidden' : ''}`} title={message}>
        {message}
      </span>
      <div className={`nodes-error-full${expanded ? ' nodes-error-full--visible' : ''}`}>
        {message}
      </div>
      {isLong && (
        <button
          className="nodes-error-toggle"
          onClick={e => { e.stopPropagation(); setExpanded(v => !v); }}
          onKeyDown={e => { e.stopPropagation(); }}
        >
          {expanded ? 'less' : 'more'}
        </button>
      )}
    </div>
  );
}

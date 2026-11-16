import { useState } from 'react';

// ErrorCell shows a long error text on one truncated line, with a `more`
// toggle that expands it in place (and `less` that folds it back). A text of
// 60 characters or fewer has no toggle. The click and key events of the toggle
// stop here, so a row or group header that handles them is not triggered.
export default function ErrorCell({ message }: { message: string }) {
  const [expanded, setExpanded] = useState(false);
  const isLong = message.length > 60;
  return (
    <div className="nodes-error-text">
      <span className={`nodes-error-short${expanded ? ' nodes-error-short--hidden' : ''}`}>
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

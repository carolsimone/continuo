// Node-type icons for the three node families the topology can contain.
// The SVG path data is vendored so the dashboard needs no network access to
// render it: the dbt mark comes from dbt Labs' dbt-docs repository
// (Apache-2.0), the Python logo from the simple-icons set (CC0). Both are
// trademarks of their respective owners, used here to identify the tool that
// owns a node.
const DBT_MARK_PATH =
  'M86.1754 3.74322C88.2911 5.77758 89.6745 8.46293 90 11.3924C90 12.613 89.6745 13.4268 88.9421 14.9729C88.2098 16.519 79.1772 32.1429 76.4919 36.4557C74.9458 38.9783 74.132 41.9892 74.132 44.9186C74.132 47.9295 74.9458 50.859 76.4919 53.3816C79.1772 57.6944 88.2098 73.3996 88.9421 74.9457C89.6745 76.4919 90 77.2242 90 78.4448C89.6745 81.3743 88.3725 84.0597 86.2568 86.0127C84.2224 88.1284 81.5371 89.5118 78.689 89.7559C77.4684 89.7559 76.6546 89.4304 75.1899 88.698C73.7251 87.9656 57.7758 79.1772 53.4629 76.4919C53.1374 76.3291 52.8119 76.085 52.4051 75.9222L31.085 63.3092C31.5732 67.3779 33.3635 71.2839 36.2929 74.132C36.8626 74.7016 37.4322 75.1899 38.0832 75.6781C37.5949 75.9222 37.0253 76.1664 36.5371 76.4919C32.2242 79.1772 16.519 88.2098 14.9729 88.9421C13.4268 89.6745 12.6944 90 11.3924 90C8.46293 89.6745 5.77758 88.3725 3.82459 86.2568C1.70886 84.2224 0.325497 81.5371 0 78.6076C0.0813743 77.387 0.406872 76.1664 1.05787 75.1085C1.79024 73.5624 10.8228 57.8571 13.5081 53.5443C15.0542 51.0217 15.868 48.0922 15.868 45.0814C15.868 42.0705 15.0542 39.141 13.5081 36.6184C10.8228 32.1429 1.70886 16.4376 1.05787 14.8915C0.406872 13.8336 0.0813743 12.613 0 11.3924C0.325497 8.46293 1.62749 5.77758 3.74322 3.74322C5.77758 1.62749 8.46293 0.325497 11.3924 0C12.613 0.0813743 13.8336 0.406872 14.9729 1.05787C16.2749 1.62749 27.7486 8.30018 33.8517 11.8807L35.2351 12.6944C35.7233 13.0199 36.1302 13.264 36.4557 13.4268L37.1067 13.8336L58.8336 26.6908C58.3454 21.8083 55.8228 17.3327 51.9168 14.3219C52.4051 14.0778 52.9747 13.8336 53.4629 13.5081C57.7758 10.8228 73.481 1.70886 75.0271 1.05787C76.085 0.406872 77.3056 0.0813743 78.6076 0C81.4557 0.325497 84.1411 1.62749 86.1754 3.74322ZM46.1392 50.7776L50.7776 46.1392C51.4286 45.4882 51.4286 44.5118 50.7776 43.8608L46.1392 39.2224C45.4882 38.5714 44.5118 38.5714 43.8608 39.2224L39.2224 43.8608C38.5714 44.5118 38.5714 45.4882 39.2224 46.1392L43.8608 50.7776C44.4304 51.3472 45.4882 51.3472 46.1392 50.7776Z';

const PYTHON_LOGO_PATH =
  'M14.25.18l.9.2.73.26.59.3.45.32.34.34.25.34.16.33.1.3.04.26.02.2-.01.13V8.5l-.05.63-.13.55-.21.46-.26.38-.3.31-.33.25-.35.19-.35.14-.33.1-.3.07-.26.04-.21.02H8.77l-.69.05-.59.14-.5.22-.41.27-.33.32-.27.35-.2.36-.15.37-.1.35-.07.32-.04.27-.02.21v3.06H3.17l-.21-.03-.28-.07-.32-.12-.35-.18-.36-.26-.36-.36-.35-.46-.32-.59-.28-.73-.21-.88-.14-1.05-.05-1.23.06-1.22.16-1.04.24-.87.32-.71.36-.57.4-.44.42-.33.42-.24.4-.16.36-.1.32-.05.24-.01h.16l.06.01h8.16v-.83H6.18l-.01-2.75-.02-.37.05-.34.11-.31.17-.28.25-.26.31-.23.38-.2.44-.18.51-.15.58-.12.64-.1.71-.06.77-.04.84-.02 1.27.05zm-6.3 1.98l-.23.33-.08.41.08.41.23.34.33.22.41.09.41-.09.33-.22.23-.34.08-.41-.08-.41-.23-.33-.33-.22-.41-.09-.41.09zm13.09 3.95l.28.06.32.12.35.18.36.27.36.35.35.47.32.59.28.73.21.88.14 1.04.05 1.23-.06 1.23-.16 1.04-.24.86-.32.71-.36.57-.4.45-.42.33-.42.24-.4.16-.36.09-.32.05-.24.02-.16-.01h-8.22v.82h5.84l.01 2.76.02.36-.05.34-.11.31-.17.29-.25.25-.31.24-.38.2-.44.17-.51.15-.58.13-.64.09-.71.07-.77.04-.84.01-1.27-.04-1.07-.14-.9-.2-.73-.25-.59-.3-.45-.33-.34-.34-.25-.34-.16-.33-.1-.3-.04-.25-.02-.2.01-.13v-5.34l.05-.64.13-.54.21-.46.26-.38.3-.32.33-.24.35-.2.35-.14.33-.1.3-.06.26-.04.21-.02.13-.01h5.84l.69-.05.59-.14.5-.21.41-.28.33-.32.27-.35.2-.36.15-.36.1-.35.07-.32.04-.28.02-.21V6.07h2.09l.14.01zm-6.47 14.25l-.23.33-.08.41.08.41.23.33.33.23.41.08.41-.08.33-.23.23-.33.08-.41-.08-.41-.23-.33-.33-.23-.41-.08-.41.08z';

// A plain data-table glyph; python-csv has no official mark, so it renders
// the Python logo with this badge overlaid.
const TABLE_BADGE_PATH =
  'M3 3h18v18H3V3zm2 2v4h6V5H5zm8 0v4h6V5h-6zM5 11v4h6v-4H5zm8 0v4h6v-4h-6zM5 17v2h6v-2H5zm8 0v2h6v-2h-6z';

export type NodeTypeFamily = 'dbt' | 'python' | 'python-csv';

// Collapses the exact node_type ("dbt-model", "dbt-seed", ...) to the family
// that decides which icon is shown. Unknown or empty types map to null and
// render no icon.
export function nodeTypeFamily(nodeType: string): NodeTypeFamily | null {
  if (nodeType.startsWith('dbt-')) return 'dbt';
  if (nodeType === 'python-model') return 'python';
  if (nodeType === 'python-csv') return 'python-csv';
  return null;
}

function DbtMark({ size }: { size: number }) {
  return (
    <svg
      data-node-type-icon="dbt"
      role="img"
      aria-label="dbt"
      width={size}
      height={size}
      viewBox="0 0 90 90"
    >
      <path fill="#FF694A" d={DBT_MARK_PATH} />
    </svg>
  );
}

function PythonMark({ size, ...rest }: { size: number } & Record<string, unknown>) {
  return (
    <svg role="img" aria-label="python" width={size} height={size} viewBox="0 0 24 24" {...rest}>
      <path fill="#3776AB" d={PYTHON_LOGO_PATH} />
    </svg>
  );
}

interface Props {
  nodeType: string;
  size?: number;
}

export default function NodeTypeIcon({ nodeType, size = 14 }: Props) {
  const family = nodeTypeFamily(nodeType);
  if (family === 'dbt') return <DbtMark size={size} />;
  if (family === 'python') return <PythonMark size={size} data-node-type-icon="python" />;
  if (family === 'python-csv') {
    return (
      <span
        data-node-type-icon="python-csv"
        className="node-type-csv"
        role="img"
        aria-label="python csv"
        style={{ width: size, height: size }}
      >
        <PythonMark size={size} />
        <svg
          className="node-type-csv-badge"
          width={Math.round(size * 0.57)}
          height={Math.round(size * 0.57)}
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path fill="#475569" d={TABLE_BADGE_PATH} />
        </svg>
      </span>
    );
  }
  return null;
}

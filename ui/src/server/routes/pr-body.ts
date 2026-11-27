// buildPullRequestBody renders the body of a remediation pull request. The
// diff comes before the rationale: the diff is the change a reviewer judges,
// the rationale is the facts around it plus the model's one-line note.

// What became of the review of one owner: GitHub took the request, GitHub
// refused it, or the owner is not a GitHub user or team and is only named.
export type OwnerReview = 'requested' | 'refused' | 'listed';

// One node's owner and what became of its review. An empty owner is a node
// with no owner.
export interface PullRequestOwner {
  nodeId: string;
  owner: string;
  review: OwnerReview;
}

const OWNER_REVIEW_TEXT: Record<OwnerReview, string> = {
  requested: 'review requested',
  refused: 'review request refused',
  listed: 'listed only',
};

interface PullRequestFile {
  path: string;
  renamed_from?: string;
  target_node_id?: string;
}

export interface PullRequestBodyInput {
  nodeIds: string[];
  releaseId: string;
  errorSignature?: string;
  model?: string;
  confidence?: string;
  // Each file the pull request writes. renamed_from is the path a moved file
  // leaves; target_node_id the node the file's change fixes.
  files: PullRequestFile[];
  rationale?: string;
  diff?: string;
  // The owner of each fixed node, in the order to list them. Left out of the
  // body when empty, which is a claim that records no owners.
  owners?: PullRequestOwner[];
  // The continuo UI's external base URL, without a trailing slash. GitHub
  // renders a pull request body against github.com, so the link back to the
  // Remediation tab must be absolute; with no URL the link is left out.
  uiUrl?: string;
}

// ownerLine states what became of one node's owner: asked to review, refused
// by GitHub, only named (an owner GitHub cannot be asked for), or absent. An
// owner is text a repository wrote, so it is held to one line.
function ownerLine({ nodeId, owner, review }: PullRequestOwner): string {
  const name = owner.replace(/\s+/g, ' ').trim();
  if (name === '') return `- \`${nodeId}\`: no owner`;
  return `- \`${nodeId}\`: ${name} (${OWNER_REVIEW_TEXT[review]})`;
}

// fileLine names one file the pull request writes, then in parentheses the
// path it was renamed from and the node it fixes, each when known:
// - `new.sql` (renamed from `old.sql`; fixes `a.b`)
function fileLine({ path, renamed_from, target_node_id }: PullRequestFile): string {
  const notes = [
    renamed_from ? `renamed from \`${renamed_from}\`` : null,
    target_node_id ? `fixes \`${target_node_id}\`` : null,
  ].filter((note): note is string => note !== null);
  return `- \`${path}\`${notes.length > 0 ? ` (${notes.join('; ')})` : ''}`;
}

export function buildPullRequestBody(input: PullRequestBodyInput): string {
  const lines: (string | null)[] = [
    `## Automated fix`,
    ``,
    `**Nodes:** ${input.nodeIds.map((n) => `\`${n}\``).join(', ')}`,
    `**Release:** \`${input.releaseId}\``,
    input.errorSignature ? `**Error signature:** ${input.errorSignature}` : null,
    input.model ? `**Model:** ${input.model}` : null,
    input.confidence !== undefined ? `**Confidence:** ${input.confidence}` : null,
    ``,
    `### Files changed`,
    ...input.files.map(fileLine),
    input.owners && input.owners.length > 0 ? `\n### Owners\n${input.owners.map(ownerLine).join('\n')}` : null,
    input.diff ? `\n### Changes\n\`\`\`diff\n${input.diff}\n\`\`\`` : null,
    input.rationale ? `\n### Rationale\n${input.rationale}` : null,
    ``,
    `---`,
    `*Proposed by the automated remediation agent — review before merge.*`,
    input.uiUrl ? `` : null,
    input.uiUrl ? `[View in continuo UI](${input.uiUrl}/?tab=remediation)` : null,
  ];
  return lines.filter((line): line is string => line !== null).join('\n').trim();
}

// buildPullRequestBody renders the body of a remediation pull request. The
// diff comes before the rationale: the diff is the change a reviewer judges,
// the rationale is the facts around it plus the model's one-line note.

// One node's owner and whether GitHub was asked to have that owner review the
// pull request. An empty owner is a node with no owner.
export interface PullRequestOwner {
  nodeId: string;
  owner: string;
  reviewRequested: boolean;
}

export interface PullRequestBodyInput {
  nodeIds: string[];
  releaseId: string;
  errorSignature?: string;
  model?: string;
  confidence?: string;
  files: { path: string; target_node_id?: string }[];
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

// ownerLine states what became of one node's owner: asked to review, only
// named (an owner GitHub cannot be asked for), or absent. An owner is text a
// repository wrote, so it is held to one line.
function ownerLine({ nodeId, owner, reviewRequested }: PullRequestOwner): string {
  const name = owner.replace(/\s+/g, ' ').trim();
  if (name === '') return `- \`${nodeId}\`: no owner`;
  return `- \`${nodeId}\`: ${name} (${reviewRequested ? 'review requested' : 'listed only'})`;
}

export function buildPullRequestBody(input: PullRequestBodyInput): string {
  const lines: (string | null)[] = [
    `## Automated remediation proposal`,
    ``,
    `**Nodes:** ${input.nodeIds.map((n) => `\`${n}\``).join(', ')}`,
    `**Release:** \`${input.releaseId}\``,
    input.errorSignature ? `**Error signature:** ${input.errorSignature}` : null,
    input.model ? `**Model:** ${input.model}` : null,
    input.confidence !== undefined ? `**Confidence:** ${input.confidence}` : null,
    ``,
    `### Files changed`,
    ...input.files.map((file) => `- \`${file.path}\`${file.target_node_id ? ` (fixes \`${file.target_node_id}\`)` : ''}`),
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

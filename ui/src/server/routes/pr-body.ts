// buildPullRequestBody renders the body of a remediation pull request. The
// diff comes before the rationale: the diff is the change a reviewer judges,
// the rationale is the facts around it plus the model's one-line note.
export interface PullRequestBodyInput {
  nodeIds: string[];
  releaseId: string;
  errorSignature?: string;
  model?: string;
  confidence?: string;
  files: { path: string; target_node_id?: string }[];
  rationale?: string;
  diff?: string;
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
    input.diff ? `\n### Changes\n\`\`\`diff\n${input.diff}\n\`\`\`` : null,
    input.rationale ? `\n### Rationale\n${input.rationale}` : null,
    ``,
    `---`,
    `*Proposed by the automated remediation agent — review before merge.*`,
    ``,
    `[View in Continuo UI](/?tab=remediation)`,
  ];
  return lines.filter((line): line is string => line !== null).join('\n').trim();
}

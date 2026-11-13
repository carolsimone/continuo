// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, fireEvent, screen, waitFor } from '@testing-library/react';
import { MiniMap, ReactFlowProvider, type Node } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge } from '../../src/client/types';

// jsdom measures no node, so the real minimap draws none of them. The mock
// records the colour function the panel hands the minimap and renders the real
// minimap unchanged.
const minimap = vi.hoisted(() => ({ nodeColor: undefined as undefined | ((node: Node) => string) }));
vi.mock('@xyflow/react', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@xyflow/react')>();
  return {
    ...actual,
    MiniMap: (props: React.ComponentProps<typeof MiniMap>) => {
      minimap.nodeColor = props.nodeColor as (node: Node) => string;
      return <actual.MiniMap {...props} />;
    },
  };
});

// A dormant node is a model continuo builds that has no schedule tag: the
// schedule graph shows it as the upstream of a node it feeds.
const DORMANT_ID = 's.a.dormant_m';
const READER_ID = 's.a.reader';

function dormantNode(lastRunAt: string | null = null): GraphNode {
  return { node_id: DORMANT_ID, node_type: 'dbt-model', schedule_name: '', dormant: true, last_run_at: lastRunAt };
}
const READER: GraphNode = { node_id: READER_ID, node_type: 'dbt-model', schedule_name: 's', dormant: false, last_run_at: null };
const EDGES: GraphEdge[] = [{ from_node_id: DORMANT_ID, to_node_id: READER_ID }];

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function nodeIn(container: HTMLElement, id: string): HTMLElement {
  return container.querySelector(`.react-flow__node[data-id="${id}"]`) as HTMLElement;
}

function panel(graphNodes: GraphNode[], onNodeClick: (nodeId: string | null) => void = () => {}) {
  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={graphNodes}
        graphEdges={EDGES}
        tasks={[]}
        selectedNodeId={null}
        onNodeClick={onNodeClick}
        colorByStatus={false}
      />
    </ReactFlowProvider>
  );
}

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

// Answers each run POST with the next reply ('timeout' rejects, as a request
// that got no answer does) and returns the request ids the POSTs carried.
function installRun(replies: Array<'timeout' | { status: number; body: unknown }>) {
  const posts: { url: string; body: Record<string, unknown> }[] = [];
  mockFetch.mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input.toString();
    const body = JSON.parse(String(init?.body));
    posts.push({ url, body });
    const reply = replies[posts.length - 1];
    if (reply === 'timeout') return Promise.reject(new Error('timeout'));
    return Promise.resolve({ ok: reply.status < 300, status: reply.status, json: async () => reply.body } as Response);
  });
  return posts;
}

describe('DAGPanel dormant nodes', () => {
  it('draws a dormant node light grey, solid, with its type icon and a dormant chip', () => {
    const { container } = render(panel([dormantNode(), READER]));
    const node = nodeIn(container, DORMANT_ID);

    expect(node.style.background).toBe('rgb(241, 245, 249)');
    expect(node.style.borderTopStyle).toBe('solid');
    expect(node.style.borderTopColor).toBe('rgb(148, 163, 184)');
    expect(node.style.color).toBe('rgb(71, 85, 105)');
    expect(node.querySelector('[data-node-type-icon]')?.getAttribute('data-node-type-icon')).toBe('dbt');
    expect(node.querySelector('.dag-node-label > span')?.textContent).toBe('dormant_m');
    expect(node.querySelector('.dag-node-chip--dormant')?.textContent).toBe('dormant');
    expect(node.querySelector('button.btn')?.textContent).toBe('▶');

    // A node with a schedule keeps its usual look: no chip, no ▶.
    const reader = nodeIn(container, READER_ID);
    expect(reader.querySelector('.dag-node-chip')).toBeNull();
    expect(reader.querySelector('button')).toBeNull();
    expect(reader.getAttribute('title')).toBeNull();
  });

  it('shows the dormant tooltip with the last run, and "never" when it never ran', () => {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { container, rerender } = render(panel([dormantNode(fiveMinutesAgo), READER]));
    expect(nodeIn(container, DORMANT_ID).getAttribute('title'))
      .toBe('No schedule tag: runs only by hand. Last run 5m ago.');

    rerender(panel([dormantNode(null), READER]));
    expect(nodeIn(container, DORMANT_ID).getAttribute('title'))
      .toBe('No schedule tag: runs only by hand. Last run: never.');
  });

  it('starts a single-node run with one request id when ▶ is clicked, and reuses it on retry', async () => {
    const posts = installRun(['timeout', { status: 200, body: { run_id: 'r1', schedule_name: 'single-node-run-1' } }]);
    const onNodeClick = vi.fn();
    const { container } = render(panel([dormantNode(), READER], onNodeClick));
    const run = () => nodeIn(container, DORMANT_ID).querySelector('button') as HTMLButtonElement;

    fireEvent.click(run());
    await waitFor(() => expect(posts).toHaveLength(1));
    expect(posts[0].url).toBe('/api/nodes/s/a/dormant_m/run');
    expect(posts[0].body).toEqual({ operation: '', request_id: expect.stringMatching(UUID) });
    // The request got no answer: the error is said above the canvas, naming the node.
    const strip = await screen.findByText('Could not run dormant_m: Request failed — please try again');
    expect(strip.className).toContain('info-strip--error');

    fireEvent.click(run());
    await waitFor(() => expect(posts).toHaveLength(2));
    expect(posts[1].body.request_id).toBe(posts[0].body.request_id);
    await waitFor(() => expect(run().className).toContain('is-success'));
    expect(screen.queryByText(/Could not run/)).toBeNull();

    // A press on ▶ runs the node; it neither selects it nor opens anything.
    expect(onNodeClick).not.toHaveBeenCalled();
  });

  it('makes a press a new action once the graph shows a run newer than the unanswered press saw', async () => {
    const posts = installRun(['timeout', { status: 200, body: { run_id: 'r2', schedule_name: 'single-node-run-2' } }]);
    const { container, rerender } = render(panel([dormantNode(null), READER]));
    const run = () => nodeIn(container, DORMANT_ID).querySelector('button') as HTMLButtonElement;

    fireEvent.click(run());
    await screen.findByText(/Could not run dormant_m/);
    rerender(panel([dormantNode(new Date().toISOString()), READER]));
    fireEvent.click(run());

    await waitFor(() => expect(posts).toHaveLength(2));
    expect(posts[1].body.request_id).toMatch(UUID);
    expect(posts[1].body.request_id).not.toBe(posts[0].body.request_id);
  });

  it('says why a refused run did not start', async () => {
    installRun([{ status: 400, body: { error: 'node s.a.dormant_m is not in the live release' } }]);
    const { container } = render(panel([dormantNode(), READER]));

    fireEvent.click(nodeIn(container, DORMANT_ID).querySelector('button') as HTMLButtonElement);

    expect(await screen.findByText('Could not run dormant_m: node s.a.dormant_m is not in the live release'))
      .toBeInTheDocument();
  });

  it('a click on a dormant node, not on its ▶, selects it as any node', () => {
    const onNodeClick = vi.fn();
    const { container } = render(panel([dormantNode(), READER], onNodeClick));

    fireEvent.click(nodeIn(container, DORMANT_ID));

    expect(onNodeClick).toHaveBeenCalledWith(DORMANT_ID);
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('draws a dormant node light grey in the minimap', () => {
    render(panel([dormantNode(), READER]));
    expect(minimap.nodeColor?.({ data: { dormant: true }, style: { background: '#f1f5f9' } } as unknown as Node))
      .toBe('#f1f5f9');
    // A skipped node shares the fill and keeps the minimap's plain grey.
    expect(minimap.nodeColor?.({ data: {}, style: { background: '#f1f5f9' } } as unknown as Node))
      .toBe('#e5e7eb');
  });
});

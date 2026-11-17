// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import NodeContractPanel from '../../src/client/NodeContractPanel';
import type { ContractFieldChange, ContractVersion, NodeContractResponse } from '../../src/client/types';

const mockFetch = vi.fn();
beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

function jsonResp(body: unknown, status = 200) {
  return Promise.resolve({ ok: status >= 200 && status < 300, status, json: async () => body } as Response);
}

// A response the test answers by hand, when it chooses.
function deferred() {
  let resolve!: (r: Response) => void;
  const promise = new Promise<Response>((res) => { resolve = res; });
  return { promise, resolve };
}

const CONTRACT_URL = '/api/nodes/py-service/analytics/py_daily_kpis/contract';
const diffUrl = (from: number, to: number) => `${CONTRACT_URL}/diff?from=${from}&to=${to}`;

const YAML_TEXT = [
  'apiVersion: v3.2.0',
  'kind: DataContract',
  'id: analytics.py_daily_kpis',
  'version: "3"',
  'team:',
  '  name: service-py',
  '',
].join('\n');

const PROMOTED_AT: Record<number, string> = {
  1: '2026-11-13T09:02:44Z',
  2: '2026-11-20T11:15:03Z',
  3: '2026-11-27T16:40:12Z',
};
const RELEASE: Record<number, string> = {
  1: 'rel-4e5f6a7-212',
  2: 'rel-5d6e7f8-219',
  3: 'rel-77aa0bc-230',
};

// A node whose version 2 changed the team and whose version 3 changed it back,
// so version 3 repeats version 1's contract.
const version = (n: number, over: Partial<ContractVersion> = {}): ContractVersion => ({
  unique_id: 'analytics.py_daily_kpis',
  version: n,
  contract_hash: `sha256:contract-${n}`,
  release_id: RELEASE[n] ?? `rel-${n}`,
  promoted_at: PROMOTED_AT[n] ?? '2026-12-01T10:00:00Z',
  content_hash: 'sha256:c',
  is_current: false,
  same_as_version: null,
  continuo_contracts: '0.2.0',
  ontology_version: '',
  ...over,
});

const v3 = version(3, { is_current: true, same_as_version: 1, document_yaml: YAML_TEXT });
const designHistory: NodeContractResponse = {
  contract: v3,
  versions: [{ ...v3, document_yaml: undefined }, version(2), version(1)],
};

const teamChange: ContractFieldChange = {
  path: 'team.name', before_json: '"service-py"', after_json: '"analytics-eng"',
};

function serve(contract: NodeContractResponse, diffs: Record<string, ContractFieldChange[]> = {}) {
  mockFetch.mockImplementation((input: RequestInfo | URL) => {
    const url = String(input);
    if (url === CONTRACT_URL) return jsonResp(contract);
    const diff = Object.entries(diffs).find(([u]) => u === url);
    if (diff) return jsonResp({ changes: diff[1] });
    return jsonResp({ error: `unexpected ${url}` }, 404);
  });
}

function renderPanel(table = 'py_daily_kpis') {
  return render(
    <MemoryRouter>
      <NodeContractPanel service="py-service" schema="analytics" table={table} />
    </MemoryRouter>,
  );
}

describe('NodeContractPanel', () => {
  it('shows the current contract as YAML with its version, release and date', async () => {
    serve(designHistory);
    const { container } = renderPanel();

    const current = await screen.findByRole('region', { name: 'Current contract' });
    expect(container.querySelector('pre')).toHaveTextContent('id: analytics.py_daily_kpis');
    expect(container.querySelector('pre')?.textContent).toBe(YAML_TEXT);
    expect(within(current).getByText('Version 3')).toBeInTheDocument();
    expect(within(current).getByRole('link', { name: 'rel-77aa0bc-230' })).toHaveAttribute('href', '/releases/rel-77aa0bc-230');
    expect(within(current).getByText(new Date(PROMOTED_AT[3]).toLocaleString())).toBeInTheDocument();
  });

  it('lists every version with its release and date, "same as version 1" on the revert and "current" on the newest', async () => {
    serve(designHistory);
    renderPanel();

    const history = await screen.findByRole('region', { name: 'Version history' });
    const rows = within(history).getAllByRole('row').slice(1);
    expect(rows).toHaveLength(3);

    expect(within(rows[0]).getByText('3')).toBeInTheDocument();
    expect(within(rows[0]).getByText('same as version 1')).toBeInTheDocument();
    expect(within(rows[0]).getByText('current')).toBeInTheDocument();
    expect(within(rows[0]).getByRole('link', { name: 'rel-77aa0bc-230' })).toHaveAttribute('href', '/releases/rel-77aa0bc-230');
    expect(within(rows[0]).getByText(new Date(PROMOTED_AT[3]).toLocaleString())).toBeInTheDocument();

    expect(within(rows[1]).getByText('2')).toBeInTheDocument();
    expect(within(rows[1]).queryByText(/same as/)).not.toBeInTheDocument();
    expect(within(rows[1]).queryByText('current')).not.toBeInTheDocument();
    expect(within(rows[2]).getByRole('link', { name: 'rel-4e5f6a7-212' })).toBeInTheDocument();

    expect(within(history).getAllByText(/same as version/)).toHaveLength(1);
    expect(within(history).getAllByText('current')).toHaveLength(1);
  });

  it('shows the field two picked versions differ at', async () => {
    serve(designHistory, { [diffUrl(1, 2)]: [teamChange] });
    renderPanel();
    const user = userEvent.setup();

    const compare = await screen.findByRole('region', { name: 'Compare versions' });
    expect(within(compare).queryByRole('table')).not.toBeInTheDocument();
    await user.selectOptions(within(compare).getByLabelText('From'), '1');
    expect(mockFetch).not.toHaveBeenCalledWith(diffUrl(1, 2), expect.anything());
    await user.selectOptions(within(compare).getByLabelText('To'), '2');

    const row = (await within(compare).findByText('team.name')).closest('tr') as HTMLElement;
    expect(within(row).getByText('"service-py"')).toBeInTheDocument();
    expect(within(row).getByText('"analytics-eng"')).toBeInTheDocument();
    expect(within(compare).getAllByRole('columnheader').map((h) => h.textContent)).toEqual(['Path', 'Before', 'After']);
    expect(mockFetch).toHaveBeenCalledWith(diffUrl(1, 2), expect.anything());
  });

  it('says two versions with the same contract differ nowhere', async () => {
    serve(designHistory, { [diffUrl(1, 3)]: [] });
    renderPanel();
    const user = userEvent.setup();

    const compare = await screen.findByRole('region', { name: 'Compare versions' });
    await user.selectOptions(within(compare).getByLabelText('From'), '1');
    await user.selectOptions(within(compare).getByLabelText('To'), '3');

    expect(await within(compare).findByText('Version 1 and version 3 have the same contract.')).toBeInTheDocument();
    expect(within(compare).queryByRole('table')).not.toBeInTheDocument();
  });

  it('marks a field one version lacks with a dash', async () => {
    serve(designHistory, {
      [diffUrl(2, 3)]: [{ path: 'schema[0].properties[2]', before_json: '', after_json: '{"name":"orders"}' }],
    });
    renderPanel();
    const user = userEvent.setup();

    const compare = await screen.findByRole('region', { name: 'Compare versions' });
    await user.selectOptions(within(compare).getByLabelText('From'), '2');
    await user.selectOptions(within(compare).getByLabelText('To'), '3');

    const row = (await within(compare).findByText('schema[0].properties[2]')).closest('tr') as HTMLElement;
    const cells = within(row).getAllByRole('cell');
    expect(cells[1]).toHaveTextContent('—');
    expect(cells[2]).toHaveTextContent('{"name":"orders"}');
  });

  it('keeps the answer of the last pick when an earlier read answers later', async () => {
    const first = deferred();
    mockFetch.mockImplementation((input: RequestInfo | URL) => {
      const url = String(input);
      if (url === CONTRACT_URL) return jsonResp(designHistory);
      if (url === diffUrl(1, 2)) return first.promise;
      if (url === diffUrl(1, 3)) return jsonResp({ changes: [{ path: 'owner', before_json: '"a"', after_json: '"b"' }] });
      return jsonResp({}, 404);
    });
    renderPanel();
    const user = userEvent.setup();

    const compare = await screen.findByRole('region', { name: 'Compare versions' });
    await user.selectOptions(within(compare).getByLabelText('From'), '1');
    await user.selectOptions(within(compare).getByLabelText('To'), '2');
    await user.selectOptions(within(compare).getByLabelText('To'), '3');
    expect(await within(compare).findByText('owner')).toBeInTheDocument();

    await act(async () => { first.resolve(await jsonResp({ changes: [teamChange] })); });
    expect(within(compare).getByText('owner')).toBeInTheDocument();
    expect(within(compare).queryByText('team.name')).not.toBeInTheDocument();
  });

  it('names a failed diff read in the compare card and keeps the rest of the page', async () => {
    mockFetch.mockImplementation((input: RequestInfo | URL) =>
      String(input) === CONTRACT_URL ? jsonResp(designHistory) : jsonResp({ error: 'version 9 not found' }, 404));
    renderPanel();
    const user = userEvent.setup();

    const compare = await screen.findByRole('region', { name: 'Compare versions' });
    await user.selectOptions(within(compare).getByLabelText('From'), '1');
    await user.selectOptions(within(compare).getByLabelText('To'), '2');

    expect(await within(compare).findByText(/Could not compare these versions/)).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Current contract' })).toBeInTheDocument();
  });

  it('offers no comparison for a node with one version', async () => {
    const only = version(1, { is_current: true, document_yaml: YAML_TEXT });
    serve({ contract: only, versions: [{ ...only, document_yaml: undefined }] });
    renderPanel();

    await screen.findByRole('region', { name: 'Version history' });
    expect(screen.queryByRole('region', { name: 'Compare versions' })).not.toBeInTheDocument();
  });

  it('says a node with no contract has none and shows nothing else', async () => {
    serve({ contract: null, versions: [] });
    renderPanel();

    expect(await screen.findByText(/^This node has no contract\. Only Python services released as python-odcs have contracts/)).toBeInTheDocument();
    expect(screen.queryByRole('region')).not.toBeInTheDocument();
  });

  it('keeps the history of a node whose contract is gone', async () => {
    serve({ contract: null, versions: [version(2), version(1)] });
    renderPanel();

    expect(await screen.findByText('This node has no current contract.')).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Current contract' })).not.toBeInTheDocument();
    const history = screen.getByRole('region', { name: 'Version history' });
    expect(within(history).getAllByRole('row')).toHaveLength(3);
    expect(within(history).queryByText('current')).not.toBeInTheDocument();
  });

  it('says where the kept history begins when older versions were removed', async () => {
    const kept = [version(5, { is_current: true, document_yaml: YAML_TEXT }), version(4), version(3)];
    serve({ contract: kept[0], versions: kept.map((v, i) => (i === 0 ? { ...v, document_yaml: undefined } : v)) });
    renderPanel();

    expect(await screen.findByText('Versions before 3 were removed by the contract history setting.')).toBeInTheDocument();
  });

  it('says nothing about removed versions when the history starts at version 1', async () => {
    serve(designHistory);
    renderPanel();

    await screen.findByRole('region', { name: 'Version history' });
    expect(screen.queryByText(/removed by the contract history setting/)).not.toBeInTheDocument();
  });

  it('shows a failed read as an error and not as a missing contract', async () => {
    mockFetch.mockImplementation(() => jsonResp({ error: 'catalog down' }, 500));
    renderPanel();

    expect(await screen.findByText(/Could not read this node's contract/)).toBeInTheDocument();
    expect(screen.queryByText(/^This node has no contract/)).not.toBeInTheDocument();
  });

  it('reads the contract of the node it now shows when the node changes', async () => {
    const other = '/api/nodes/py-service/analytics/py_other/contract';
    mockFetch.mockImplementation((input: RequestInfo | URL) =>
      String(input) === CONTRACT_URL ? jsonResp(designHistory) : jsonResp({ contract: null, versions: [] }));
    const { rerender } = renderPanel();
    await screen.findByRole('region', { name: 'Current contract' });

    rerender(
      <MemoryRouter>
        <NodeContractPanel service="py-service" schema="analytics" table="py_other" />
      </MemoryRouter>,
    );

    expect(await screen.findByText(/^This node has no contract\. Only Python services released as python-odcs have contracts/)).toBeInTheDocument();
    expect(mockFetch).toHaveBeenCalledWith(other, expect.anything());
    expect(screen.queryByRole('region', { name: 'Current contract' })).not.toBeInTheDocument();
  });
});

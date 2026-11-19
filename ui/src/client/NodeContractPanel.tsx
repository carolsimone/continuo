import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { formatDate } from './detail-page-helpers';
import { readJson } from './read-json';
import type {
  ContractFieldChange,
  ContractVersion,
  NodeContractDiffResponse,
  NodeContractResponse,
} from './types';

type Props = {
  service: string;
  schema: string;
  table: string;
};

type ContractRead =
  | { state: 'loading' }
  | { state: 'failed' }
  | { state: 'ready'; data: NodeContractResponse };

// The answer for one pair of versions. `key` names the pair the answer is for,
// so an answer is never shown beside a pick it does not belong to.
type DiffRead =
  | { state: 'failed'; key: string }
  | { state: 'ready'; key: string; changes: ContractFieldChange[] };

function ReleaseLink({ releaseId }: { releaseId: string }) {
  if (!releaseId) return <span className="nodes-dash">—</span>;
  return (
    <Link className="contract-release-link" to={`/releases/${encodeURIComponent(releaseId)}`}>{releaseId}</Link>
  );
}

function VersionHistory({ versions }: { versions: ContractVersion[] }) {
  const oldest = versions.length > 0 ? versions[versions.length - 1].version : 1;
  return (
    <section className="detail-card" aria-label="Version history">
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Version history</span>
          <span className="section-header__count">{versions.length}</span>
        </div>
        {oldest > 1 && (
          <div className="section-header__sub">
            {`Versions before ${oldest} were removed by the contract history setting.`}
          </div>
        )}
      </div>
      <div className="contract-table-wrap">
        <table className="nodes-table">
          <thead>
            <tr>
              <th>Version</th>
              <th>Promoted</th>
              <th>Release</th>
              <th>Note</th>
            </tr>
          </thead>
          <tbody>
            {versions.map(v => (
              <tr key={v.version} className="nodes-row--static">
                <td>{v.version}</td>
                <td>{formatDate(v.promoted_at)}</td>
                <td><ReleaseLink releaseId={v.release_id} /></td>
                <td>
                  {v.is_current && <span className="pill-sm pill-sm--current">current</span>}
                  {v.same_as_version !== null && (
                    <span className="pill-sm pill-sm--muted">{`same as version ${v.same_as_version}`}</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ChangesTable({ changes }: { changes: ContractFieldChange[] }) {
  const value = (json: string) =>
    json === '' ? <span className="nodes-dash">—</span> : <code className="contract-json">{json}</code>;
  return (
    <div className="contract-table-wrap">
      <table className="nodes-table">
        <thead>
          <tr>
            <th>Path</th>
            <th>Before</th>
            <th>After</th>
          </tr>
        </thead>
        <tbody>
          {changes.map(c => (
            <tr key={c.path} className="nodes-row--static">
              <td><code className="contract-json contract-path">{c.path}</code></td>
              <td>{value(c.before_json)}</td>
              <td>{value(c.after_json)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CompareVersions({ base, versions }: { base: string; versions: ContractVersion[] }) {
  // The versions picked, as the select holds them: '' until one is chosen.
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [diff, setDiff] = useState<DiffRead | null>(null);

  const pairKey = from !== '' && to !== '' ? `${from}:${to}` : '';

  // Each pick of a pair reads its diff once. A pick that changes the pair
  // aborts the read before it, so an older answer never lands over a newer one.
  useEffect(() => {
    if (pairKey === '') return;
    const read = new AbortController();
    readJson<NodeContractDiffResponse>(`${base}/diff?from=${from}&to=${to}`, read.signal)
      .then(data => {
        if (!read.signal.aborted) setDiff({ state: 'ready', key: pairKey, changes: data.changes ?? [] });
      })
      .catch(() => {
        if (!read.signal.aborted) setDiff({ state: 'failed', key: pairKey });
      });
    return () => read.abort();
  }, [base, from, to, pairKey]);

  const shown = diff !== null && diff.key === pairKey ? diff : null;

  const picker = (label: string, id: string, value: string, set: (v: string) => void) => (
    <div className="form-field">
      <label htmlFor={id}>{label}</label>
      <select id={id} value={value} onChange={e => set(e.target.value)}>
        <option value="">Choose…</option>
        {versions.map(v => (
          <option key={v.version} value={String(v.version)}>{`Version ${v.version}`}</option>
        ))}
      </select>
    </div>
  );

  return (
    <section className="detail-card" aria-label="Compare versions">
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Compare versions</span>
        </div>
      </div>
      <div className="contract-diff-picker">
        {picker('From', 'contract-diff-from', from, setFrom)}
        {picker('To', 'contract-diff-to', to, setTo)}
      </div>
      {pairKey !== '' && shown === null && <p className="empty">Reading the difference…</p>}
      {shown?.state === 'failed' && (
        <div className="info-strip info-strip--error">
          <span className="info-strip__icon">⚠</span>
          Could not compare these versions. Pick another pair, or reload the page.
        </div>
      )}
      {shown?.state === 'ready' && shown.changes.length === 0 && (
        <p className="empty">{`Version ${from} and version ${to} have the same contract.`}</p>
      )}
      {shown?.state === 'ready' && shown.changes.length > 0 && <ChangesTable changes={shown.changes} />}
    </section>
  );
}

// The Contract tab of a node page: the contract the node has now as the YAML
// continuo publishes, every version catalog kept, and the fields two versions
// differ at.
export default function NodeContractPanel({ service, schema, table }: Props) {
  const [read, setRead] = useState<ContractRead>({ state: 'loading' });
  const base = `/api/nodes/${encodeURIComponent(service)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/contract`;

  useEffect(() => {
    setRead({ state: 'loading' });
    const request = new AbortController();
    readJson<NodeContractResponse>(base, request.signal)
      .then(data => {
        if (request.signal.aborted) return;
        // An answer without the two fields is read as a node with no contract.
        setRead({ state: 'ready', data: { contract: data.contract ?? null, versions: data.versions ?? [] } });
      })
      .catch(() => {
        if (!request.signal.aborted) setRead({ state: 'failed' });
      });
    return () => request.abort();
  }, [base]);

  if (read.state === 'loading') return <p className="empty">Reading the contract…</p>;
  if (read.state === 'failed') {
    return (
      <div className="info-strip info-strip--error">
        <span className="info-strip__icon">⚠</span>
        Could not read this node's contract. Reload the page to try again.
      </div>
    );
  }

  const { contract, versions } = read.data;
  if (contract === null && versions.length === 0) {
    return (
      <section className="detail-card">
        <p className="empty">This node has no contract. A dbt test and an unknown-external table have none.</p>
      </section>
    );
  }

  return (
    <div className="contract-panel">
      {contract === null ? (
        <div className="info-strip info-strip--neutral">
          <span className="info-strip__icon">ⓘ</span>
          This node has no current contract.
        </div>
      ) : (
        <section className="detail-card" aria-label="Current contract">
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Current contract</span>
            </div>
            <div className="section-header__sub">
              <span>{`Version ${contract.version}`}</span>
              {' · release '}
              <ReleaseLink releaseId={contract.release_id} />
              {' · '}
              <span>{formatDate(contract.promoted_at)}</span>
            </div>
          </div>
          <pre className="log-block contract-yaml">{contract.document_yaml ?? ''}</pre>
        </section>
      )}
      <VersionHistory versions={versions} />
      {versions.length > 1 && <CompareVersions base={base} versions={versions} />}
    </div>
  );
}

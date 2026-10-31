import { useState, type ComponentProps } from 'react';
import { useActiveTab } from '../Tabs';
import Tabs from '../Tabs';
import PastRunsPanel from '../PastRunsPanel';
import RunNodeTable from '../RunNodeTable';
import RunProgressHeader from '../RunProgressHeader';
import RunSwimlane from '../RunSwimlane';
import type { runTabState } from '../detail-page-helpers';
import type { ScheduleGraph, Task, TaskAttempt } from '../types';

// Adds or removes a key from a Set without mutating the original — used for the
// swimlane's per-lane collapse state, which is toggled by lane label clicks.
function toggleSet(set: Set<string>, key: string): Set<string> {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

// The run layout: full-width tabs, status first (Run) then history (Past
// runs). The Run tab summarises the shown run's task set as a list or a
// swimlane, or says why there is none to summarise yet; the Past runs tab
// lists the schedule's runs.
export default function RunView({
  state, selectedRunId, tasks, attemptsByTask, activeGraph, services, expandedServices, onServiceToggle, pastRuns,
}: {
  state: ReturnType<typeof runTabState>;
  selectedRunId: string | null;
  tasks: Task[];
  attemptsByTask: Map<string, TaskAttempt[]>;
  activeGraph: ScheduleGraph | null;
  services: string[];
  expandedServices: Set<string>;
  onServiceToggle: (service: string) => void;
  pastRuns: ComponentProps<typeof PastRunsPanel>;
}) {
  const [runView, setRunView] = useState<'list' | 'graph'>('list');
  const [laneCollapsed, setLaneCollapsed] = useState<Set<string>>(new Set());
  const pageTabSpecs = [
    { slug: 'run', label: 'Run' },
    { slug: 'runs', label: 'Past runs', count: pastRuns.total },
  ];
  const activePage = useActiveTab('panel', 'run', pageTabSpecs.map(t => t.slug));

  return (
    <div className="detail-run">
      <Tabs variant="page" param="panel" defaultSlug="run" tabs={pageTabSpecs} />
      {activePage === 'run' && (
        <div className="run-view">
          {state === 'no-runs' ? (
            <div className="graph-empty-state">
              <p className="graph-empty-title">No runs yet.</p>
              <p className="graph-empty-copy">This schedule has not run.</p>
            </div>
          ) : state === 'loading' ? (
            <div className="graph-empty-state">
              <p className="graph-empty-title">Loading run…</p>
              <p className="graph-empty-copy">
                {selectedRunId ? 'Fetching the historical run snapshot…' : 'Fetching the latest run…'}
              </p>
            </div>
          ) : (
            <>
              <div className="seg-toggle">
                <button className={runView === 'list' ? 'on' : ''} onClick={() => setRunView('list')}>List</button>
                <button className={runView === 'graph' ? 'on' : ''} onClick={() => setRunView('graph')}>Graph</button>
              </div>
              <RunProgressHeader tasks={tasks} />
              {runView === 'list' ? (
                <RunNodeTable
                  tasks={tasks}
                  attemptsByTask={attemptsByTask}
                  edges={activeGraph?.edges ?? []}
                  expandedServices={expandedServices}
                  onServiceToggle={onServiceToggle}
                />
              ) : (
                <RunSwimlane
                  graph={activeGraph ?? { nodes: [], edges: [] }}
                  tasks={tasks}
                  serviceOrder={services}
                  collapsed={laneCollapsed}
                  onLaneToggle={(s) => setLaneCollapsed((prev) => toggleSet(prev, s))}
                />
              )}
            </>
          )}
        </div>
      )}
      {activePage === 'runs' && <PastRunsPanel {...pastRuns} />}
    </div>
  );
}

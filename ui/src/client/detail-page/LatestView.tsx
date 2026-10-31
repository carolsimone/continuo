import type { ComponentProps } from 'react';
import { useNavigate } from 'react-router';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../DAGPanel';
import NodeTypeIcon from '../NodeTypeIcon';
import PastRunsPanel from '../PastRunsPanel';
import type { GraphState } from '../detail-page-helpers';
import type { ScheduleGraph } from '../types';

// The latest layout, a topology-catalog view: the schedule's current
// dependency graph on the left and its run history on the right, with no
// live run status to summarise.
export default function LatestView({
  name, activeGraph, graphCardState, graphBadge, graphBadgeClass, selectedNodeId, selectedRunId, lastRunId,
  expandedServices, serviceColors, onNodeSelect, onServiceToggle, pastRuns,
}: {
  name: string | undefined;
  activeGraph: ScheduleGraph | null;
  graphCardState: GraphState;
  graphBadge: string;
  graphBadgeClass: string;
  selectedNodeId: string | null;
  selectedRunId: string | null;
  lastRunId: string | null | undefined;
  expandedServices: Set<string>;
  serviceColors: ComponentProps<typeof DAGPanel>['serviceColors'];
  onNodeSelect: (nodeId: string | null) => void;
  onServiceToggle: (service: string) => void;
  pastRuns: ComponentProps<typeof PastRunsPanel>;
}) {
  const navigate = useNavigate();

  const graphEmptyMessage = selectedRunId
    ? 'No DAG snapshot is available for this run.'
    : lastRunId
      ? 'The run is active, but its DAG snapshot is not available yet.'
      : 'This schedule does not have a dependency graph yet.';
  const graphErrorMessage = selectedRunId
    ? 'Failed to load the historical DAG snapshot.'
    : 'Failed to load the dependency graph.';

  const legendParentIds = selectedNodeId && activeGraph
    ? new Set(activeGraph.edges.filter((e) => e.to_node_id === selectedNodeId).map((e) => e.from_node_id))
    : new Set<string>();

  const selectedNodeType = selectedNodeId && activeGraph
    ? activeGraph.nodes.find((n) => n.node_id === selectedNodeId)?.node_type ?? ''
    : '';

  const legendChildIds = selectedNodeId && activeGraph
    ? new Set(activeGraph.edges.filter((e) => e.from_node_id === selectedNodeId).map((e) => e.to_node_id))
    : new Set<string>();

  return (
    <div className="detail-layout">
      <section className="detail-card detail-graph-card">
        <div className="detail-card-header">
          Dependency Graph
          <span className={graphBadgeClass}>{graphBadge}</span>
        </div>
        <div className="dag-card-body">
          {graphCardState === 'ready' && activeGraph ? (
            <>
              <ReactFlowProvider>
                <DAGPanel
                  graphNodes={activeGraph.nodes}
                  graphEdges={activeGraph.edges}
                  tasks={[]}
                  selectedNodeId={selectedNodeId}
                  onNodeClick={onNodeSelect}
                  colorByStatus={false}
                  serviceView={false}
                  expandedServices={expandedServices}
                  onServiceClick={onServiceToggle}
                  serviceColors={serviceColors}
                />
              </ReactFlowProvider>
              {selectedNodeId && !selectedRunId && lastRunId && (
                <div className="dag-focus-legend">
                  <div className="dag-focus-legend-title">
                    <NodeTypeIcon nodeType={selectedNodeType} size={12} />
                    {selectedNodeId.split('.').pop()}
                  </div>
                  <div className="dag-focus-legend-row">
                    <div className="dag-focus-dot dag-focus-dot--selected" /> Selected
                  </div>
                  <div className="dag-focus-legend-row">
                    <div className="dag-focus-dot dag-focus-dot--parent" />
                    Depends on ({legendParentIds.size})
                  </div>
                  <div className="dag-focus-legend-row">
                    <div className="dag-focus-dot dag-focus-dot--child" />
                    Required by ({legendChildIds.size})
                  </div>
                  <div className="dag-focus-legend-row">
                    <div className="dag-focus-dot dag-focus-dot--dim" /> Unrelated
                  </div>
                  {selectedNodeId && name && (
                    <a
                      className="dag-focus-open-link"
                      href={`/node/${encodeURIComponent(selectedNodeId)}`}
                      onClick={e => {
                        e.preventDefault();
                        navigate(
                          `/node/${encodeURIComponent(selectedNodeId)}`,
                          { state: { from: { type: 'schedule', name, mode: 'latest' } } },
                        );
                      }}
                    >
                      Open node detail →
                    </a>
                  )}
                </div>
              )}
            </>
          ) : graphCardState === 'error' ? (
            <div className="graph-empty-state">
              <p className="graph-empty-title">Graph unavailable</p>
              <p className="graph-empty-copy">{graphErrorMessage}</p>
            </div>
          ) : graphCardState === 'empty' ? (
            <div className="graph-empty-state">
              <p className="graph-empty-title">No DAG to display</p>
              <p className="graph-empty-copy">{graphEmptyMessage}</p>
            </div>
          ) : (
            <div className="graph-empty-state">
              <p className="graph-empty-title">Loading graph</p>
              <p className="graph-empty-copy">
                {selectedRunId ? 'Fetching the historical run snapshot…' : 'Fetching the dependency graph…'}
              </p>
            </div>
          )}
        </div>
      </section>

      <div className="detail-right-col">
        <section className="detail-card">
          <div className="section-header">
            <div className="section-header__main">
              <span className="section-header__title">Past Runs</span>
              <span className="section-header__count">{pastRuns.total}</span>
            </div>
          </div>
          <PastRunsPanel {...pastRuns} />
        </section>
      </div>
    </div>
  );
}

import { useCallback, useMemo, useState } from 'react';
import { buildProjectColors, listProjects, projectOfNode } from '../project-helpers';
import type { ScheduleGraph, Task } from '../types';

// The selected node and the expanded projects of the graph and the node
// table, which share both. A graph with a single project always renders
// expanded — a lone project vertex would carry no information.
export function useProjectTree(activeGraph: ScheduleGraph | null, tasks: Task[]) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedProjects, setExpandedProjects] = useState<Set<string>>(new Set());

  const projects = useMemo(
    () => listProjects(activeGraph?.nodes ?? [], tasks),
    [activeGraph, tasks],
  );
  const projectColors = useMemo(() => buildProjectColors(projects), [projects]);
  const effectiveExpandedProjects = useMemo(
    () => (projects.length <= 1 ? new Set(projects) : expandedProjects),
    [projects, expandedProjects],
  );

  // Selecting a node (graph click, table click, or search) also expands its
  // project so the selection is visible in both panels.
  const handleNodeSelect = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
    if (nodeId) {
      const svc = projectOfNode(nodeId);
      setExpandedProjects((prev) => (prev.has(svc) ? prev : new Set([...prev, svc])));
    }
  }, []);

  const handleProjectToggle = useCallback((project: string) => {
    const isCollapsing = expandedProjects.has(project);
    setExpandedProjects((prev) => {
      const next = new Set(prev);
      if (next.has(project)) next.delete(project);
      else next.add(project);
      return next;
    });
    if (isCollapsing) {
      // Collapsing hides the project's nodes; a selection inside it would
      // point at a node no longer on screen.
      setSelectedNodeId((sel) => (sel && projectOfNode(sel) === project ? null : sel));
    }
  }, [expandedProjects]);

  // Drops the selection and collapses every project, as when another run is chosen.
  const resetTree = useCallback(() => {
    setSelectedNodeId(null);
    setExpandedProjects(new Set());
  }, []);

  return {
    selectedNodeId, projects, projectColors, effectiveExpandedProjects,
    handleNodeSelect, handleProjectToggle, resetTree,
  };
}

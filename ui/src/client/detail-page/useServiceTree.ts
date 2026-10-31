import { useCallback, useMemo, useState } from 'react';
import { buildServiceColors, listServices, serviceOfNode } from '../service-helpers';
import type { ScheduleGraph, Task } from '../types';

// The selected node and the expanded services of the graph and the node
// table, which share both. A graph with a single service always renders
// expanded — a lone service vertex would carry no information.
export function useServiceTree(activeGraph: ScheduleGraph | null, tasks: Task[]) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [expandedServices, setExpandedServices] = useState<Set<string>>(new Set());

  const services = useMemo(
    () => listServices(activeGraph?.nodes ?? [], tasks),
    [activeGraph, tasks],
  );
  const serviceColors = useMemo(() => buildServiceColors(services), [services]);
  const effectiveExpandedServices = useMemo(
    () => (services.length <= 1 ? new Set(services) : expandedServices),
    [services, expandedServices],
  );

  // Selecting a node (graph click, table click, or search) also expands its
  // service so the selection is visible in both panels.
  const handleNodeSelect = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
    if (nodeId) {
      const svc = serviceOfNode(nodeId);
      setExpandedServices((prev) => (prev.has(svc) ? prev : new Set([...prev, svc])));
    }
  }, []);

  const handleServiceToggle = useCallback((service: string) => {
    const isCollapsing = expandedServices.has(service);
    setExpandedServices((prev) => {
      const next = new Set(prev);
      if (next.has(service)) next.delete(service);
      else next.add(service);
      return next;
    });
    if (isCollapsing) {
      // Collapsing hides the service's nodes; a selection inside it would
      // point at a node no longer on screen.
      setSelectedNodeId((sel) => (sel && serviceOfNode(sel) === service ? null : sel));
    }
  }, [expandedServices]);

  // Drops the selection and collapses every service, as when another run is chosen.
  const resetTree = useCallback(() => {
    setSelectedNodeId(null);
    setExpandedServices(new Set());
  }, []);

  return {
    selectedNodeId, services, serviceColors, effectiveExpandedServices,
    handleNodeSelect, handleServiceToggle, resetTree,
  };
}

// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { ReactFlowProvider } from '@xyflow/react';
import DAGPanel from '../../src/client/DAGPanel';
import type { GraphNode, GraphEdge, Task } from '../../src/client/types';

// `fitView` is the only way the panel moves the viewport on its own, so
// spying on it is how these tests tell "the canvas re-framed itself" from
// "the canvas left the user's framing alone".
const { fitViewSpy, capturedFlowProps } = vi.hoisted(() => ({
  fitViewSpy: vi.fn(),
  capturedFlowProps: { current: null as Record<string, never> | null },
}));

vi.mock('@xyflow/react', async () => {
  const actual = await vi.importActual<typeof import('@xyflow/react')>('@xyflow/react');
  return {
    ...actual,
    useReactFlow: () => ({
      fitView: fitViewSpy,
      getViewport: () => ({ x: 0, y: 0, zoom: 1 }),
      zoomIn: vi.fn(),
      zoomOut: vi.fn(),
    }),
    ReactFlow: (props: Record<string, never>) => {
      capturedFlowProps.current = props;
      return <actual.ReactFlow {...props} />;
    },
  };
});

interface FlowProps {
  onMove: (event: MouseEvent | null, viewport: { x: number; y: number; zoom: number }) => void;
}

function flowProps(): FlowProps {
  if (!capturedFlowProps.current) throw new Error('ReactFlow was never rendered');
  return capturedFlowProps.current as unknown as FlowProps;
}

// jsdom ships no ResizeObserver. The panel observes its viewport element to
// re-fit when the container changes size, so the stub keeps the callback
// reachable and the tests fire it the way a layout change would. React Flow
// registers observers of its own for node measurement; firing those would run
// its measuring code, which needs DOM APIs jsdom does not implement. Each
// observer therefore records what it watches, and only the one watching the
// panel's own viewport element is fired.
interface StubObserver {
  callback: ResizeObserverCallback;
  targets: Element[];
}

const observers: StubObserver[] = [];

class StubResizeObserver {
  private entry: StubObserver;

  constructor(callback: ResizeObserverCallback) {
    this.entry = { callback, targets: [] };
    observers.push(this.entry);
  }

  observe(target: Element) {
    this.entry.targets.push(target);
  }

  unobserve(target: Element) {
    this.entry.targets = this.entry.targets.filter((t) => t !== target);
  }

  disconnect() {
    this.entry.targets = [];
  }
}

function resizeContainer(): void {
  const watching = observers.filter((o) =>
    o.targets.some((t) => t.classList?.contains('dag-viewport')),
  );
  if (watching.length === 0) throw new Error('nothing is observing .dag-viewport');
  act(() => {
    watching.forEach((o) => o.callback([], {} as ResizeObserver));
  });
}

// A user gesture reaches `onMove` with the originating DOM event; a
// programmatic move (fitView, zoomIn) reaches it with null. That is the only
// thing separating "the user framed this" from "we framed it for them".
function userZoomTo(zoom: number): void {
  act(() => {
    flowProps().onMove(new MouseEvent('wheel'), { x: 0, y: 0, zoom });
  });
}

function freshGraphNodes(): GraphNode[] {
  return [
    { node_id: 'core.sch.a', node_type: 'dbt-model', schedule_name: 's' },
    { node_id: 'core.sch.b', node_type: 'dbt-model', schedule_name: 's' },
  ];
}

function freshGraphEdges(): GraphEdge[] {
  return [{ from_node_id: 'core.sch.a', to_node_id: 'core.sch.b' }];
}

function panel(options: { selectedNodeId?: string | null; tasks?: Task[] } = {}) {
  return (
    <ReactFlowProvider>
      <DAGPanel
        graphNodes={freshGraphNodes()}
        graphEdges={freshGraphEdges()}
        tasks={options.tasks ?? []}
        selectedNodeId={options.selectedNodeId ?? null}
        onNodeClick={() => {}}
      />
    </ReactFlowProvider>
  );
}

describe('DAGPanel viewport', () => {
  beforeEach(() => {
    fitViewSpy.mockClear();
    observers.length = 0;
    capturedFlowProps.current = null;
    vi.stubGlobal('ResizeObserver', StubResizeObserver);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('keeps the zoom the user set when the graph container resizes', () => {
    render(panel());
    expect(fitViewSpy).toHaveBeenCalledTimes(1); // the fit on mount

    userZoomTo(1.6);
    resizeContainer();

    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  it('keeps the zoom the user set when a selection is made and then cleared', () => {
    const { rerender } = render(panel());
    userZoomTo(1.6);

    rerender(panel({ selectedNodeId: 'core.sch.a' }));
    rerender(panel({ selectedNodeId: null }));

    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  it('keeps the zoom the user set when polled task statuses change', () => {
    const { rerender } = render(panel());
    userZoomTo(0.4);

    rerender(
      panel({
        tasks: [
          {
            task_id: 'core.sch.a',
            service_name: 'core',
            schema_name: 'sch',
            table_name: 'a',
            job_name: '',
            status: 'failed',
            retry_count: 0,
            max_retries: 0,
            created_at: null,
          },
        ],
      }),
    );

    expect(fitViewSpy).toHaveBeenCalledTimes(1);
  });

  it('still re-fits on a container resize when the user has not touched the viewport', () => {
    render(panel());
    expect(fitViewSpy).toHaveBeenCalledTimes(1);

    resizeContainer();

    expect(fitViewSpy).toHaveBeenCalledTimes(2);
  });

  it('ignores a programmatic move, so its own fit does not count as the user framing the graph', () => {
    render(panel());

    act(() => {
      flowProps().onMove(null, { x: 0, y: 0, zoom: 1.5 });
    });
    resizeContainer();

    expect(fitViewSpy).toHaveBeenCalledTimes(2);
  });

  it('offers Reset layout after a zoom alone, and re-fits when it is used', () => {
    render(panel());
    expect(screen.queryByRole('button', { name: /reset layout/i })).toBeNull();

    userZoomTo(1.9);
    const reset = screen.getByRole('button', { name: /reset layout/i });

    fireEvent.click(reset);

    expect(fitViewSpy).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole('button', { name: /reset layout/i })).toBeNull();
  });

  it('resumes automatic re-framing once the view has been reset', () => {
    render(panel());
    userZoomTo(1.9);
    fireEvent.click(screen.getByRole('button', { name: /reset layout/i }));
    expect(fitViewSpy).toHaveBeenCalledTimes(2);

    resizeContainer();

    expect(fitViewSpy).toHaveBeenCalledTimes(3);
  });

  it('reports the zoom level the user moved to', () => {
    const { container } = render(panel());

    userZoomTo(1.37);

    expect(container.querySelector('.dag-zoom-level')?.textContent).toBe('137%');
  });
});

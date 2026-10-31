import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchAllPages } from '../fetch-all-pages';
import { isTerminalStatus, type DetailMode } from '../detail-page-helpers';
import type { Scheduler, Task, TaskAttempt } from '../types';

// The run the Run tab shows — a selected past run, else the latest — has
// its tasks and every attempt of every task read from run-controller. A past run is
// read once; the latest run is re-read every 5 s until it is terminal. A
// poll reads the rows alongside the run's status, so the poll that first
// sees the run terminal may hold rows from before the run ended: once that
// poll's reads have landed, the rows are read one last time. Each polled
// status of the latest run goes to onScheduler. Latest mode reads no rows.
export function useRunRows({
  lastRunId,
  selectedRunId,
  mode,
  onScheduler,
}: {
  lastRunId: string | null | undefined;
  selectedRunId: string | null;
  mode: DetailMode;
  onScheduler: (scheduler: Scheduler) => void;
}): { tasks: Task[]; attempts: TaskAttempt[]; clearRows: () => void } {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [attempts, setAttempts] = useState<TaskAttempt[]>([]);
  const onSchedulerRef = useRef(onScheduler);
  onSchedulerRef.current = onScheduler;

  // Empties the rows at once, so a newly chosen run never renders over the
  // previous run's tasks while its own are read.
  const clearRows = useCallback(() => {
    setTasks([]);
    setAttempts([]);
  }, []);

  useEffect(() => {
    const shownRunId = selectedRunId ?? lastRunId;
    if (!shownRunId) return;
    if (mode === 'latest') return;
    let cancelled = false;
    let timer: number | undefined;
    const controller = new AbortController();

    // Resolves once both reads have landed (or failed) and been applied.
    const fetchRunRows = (): Promise<void> => {
      const taskRead = fetchAllPages<Task>(`/api/runs/${shownRunId}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setTasks(all);
        })
        .catch(() => {
          if (!cancelled) setTasks([]);
        });

      const attemptRead = fetchAllPages<TaskAttempt>(`/api/runs/${shownRunId}/attempts`, 'attempts', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setAttempts(all);
        })
        .catch(() => {
          if (!cancelled) setAttempts([]);
        });

      return Promise.all([taskRead, attemptRead]).then(() => undefined);
    };

    if (selectedRunId) {
      fetchRunRows();
      return () => {
        cancelled = true;
        controller.abort();
      };
    }

    const fetchDynamic = () => {
      const schedulerRequest = fetch(`/api/runs/${shownRunId}`)
        .then((response) => response.json())
        .then((data: Scheduler) => {
          if (!cancelled) onSchedulerRef.current(data);
          return data;
        })
        .catch(() => {
          return null;
        });

      // Resolves with the run's status once its row reads have landed too.
      return Promise.all([schedulerRequest, fetchRunRows()]).then(([nextScheduler]) => nextScheduler);
    };

    fetchDynamic().then((freshScheduler) => {
      if (cancelled) return;
      if (freshScheduler && !isTerminalStatus(freshScheduler.status)) {
        timer = window.setInterval(() => {
          fetchDynamic().then((nextScheduler) => {
            if (cancelled) return;
            if (timer && nextScheduler && isTerminalStatus(nextScheduler.status)) {
              window.clearInterval(timer);
              timer = undefined;
              // The run has ended: read the rows it ended with.
              fetchRunRows();
            }
          });
        }, 5000);
      }
    });

    return () => {
      cancelled = true;
      controller.abort();
      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [lastRunId, selectedRunId, mode]);

  return { tasks, attempts, clearRows };
}

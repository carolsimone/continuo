import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchAllPages } from '../fetch-all-pages';
import { isTerminalStatus, type DetailMode } from '../detail-page-helpers';
import { readJson } from '../read-json';
import type { Scheduler, Task, TaskAttempt } from '../types';

const POLL_INTERVAL_MS = 5000;

// The run the Run tab shows — a selected past run, else the latest — has
// its tasks and every attempt of every task read from run-controller. A past run is
// read once; the latest run is re-read every 5 s until it is terminal. A
// poll reads the rows alongside the run's status, so the poll that first
// sees the run terminal may hold rows from before the run ended: once that
// poll's reads have landed, the rows are read one last time. Each polled
// status of the latest run goes to onScheduler. Latest mode reads no rows.
//
// A read that fails leaves the rows and the status as they were, sets
// readFailed, and is made again every 5 s until one lands, whichever read it
// was. tasksRead says the shown run's tasks were read at least once, which is
// what tells a run with no tasks from a run whose tasks are not known yet.
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
}): {
  tasks: Task[];
  attempts: TaskAttempt[];
  tasksRead: boolean;
  readFailed: boolean;
  clearRows: () => void;
} {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [attempts, setAttempts] = useState<TaskAttempt[]>([]);
  const [tasksRead, setTasksRead] = useState(false);
  const [readFailed, setReadFailed] = useState(false);
  const onSchedulerRef = useRef(onScheduler);
  onSchedulerRef.current = onScheduler;

  // Empties the rows at once, so a newly chosen run never renders over the
  // previous run's tasks while its own are read.
  const clearRows = useCallback(() => {
    setTasks([]);
    setAttempts([]);
    setTasksRead(false);
  }, []);

  useEffect(() => {
    setTasksRead(false);
    setReadFailed(false);
    const shownRunId = selectedRunId ?? lastRunId;
    if (!shownRunId) return;
    if (mode === 'latest') return;
    let cancelled = false;
    let timer: number | undefined;
    // A past run has ended; the latest run has once a read says so. After that
    // only its rows are left to read.
    let ended = selectedRunId !== null;
    const controller = new AbortController();

    // Resolves once both reads have settled, with whether both landed.
    const fetchRunRows = (): Promise<boolean> => {
      const taskRead = fetchAllPages<Task>(`/api/runs/${shownRunId}/tasks`, 'tasks', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) {
            setTasks(all);
            setTasksRead(true);
          }
          return true;
        })
        .catch(() => false);

      const attemptRead = fetchAllPages<TaskAttempt>(`/api/runs/${shownRunId}/attempts`, 'attempts', {
        signal: controller.signal,
      })
        .then((all) => {
          if (!cancelled) setAttempts(all);
          return true;
        })
        .catch(() => false);

      return Promise.all([taskRead, attemptRead]).then(([tasksLanded, attemptsLanded]) => tasksLanded && attemptsLanded);
    };

    // Resolves with the run's status, or null when the read failed.
    const readStatus = (): Promise<Scheduler | null> =>
      readJson<Scheduler>(`/api/runs/${shownRunId}`)
        .then((data) => {
          if (!cancelled) onSchedulerRef.current(data);
          return data;
        })
        .catch(() => null);

    const keepReading = () => {
      if (timer === undefined) timer = window.setInterval(read, POLL_INTERVAL_MS);
    };
    const stopReading = () => {
      window.clearInterval(timer);
      timer = undefined;
    };

    const read = async (): Promise<void> => {
      if (ended) {
        const rowsLanded = await fetchRunRows();
        if (cancelled) return;
        setReadFailed(!rowsLanded);
        if (rowsLanded) stopReading();
        else keepReading();
        return;
      }

      const wasPolling = timer !== undefined;
      const [status, rowsLanded] = await Promise.all([readStatus(), fetchRunRows()]);
      if (cancelled) return;
      setReadFailed(status === null || !rowsLanded);
      if (status && isTerminalStatus(status.status)) {
        ended = true;
        // The run has ended: read the rows it ended with.
        if (wasPolling) {
          void read();
          return;
        }
        if (rowsLanded) return;
      }
      keepReading();
    };
    void read();

    return () => {
      cancelled = true;
      controller.abort();
      window.clearInterval(timer);
    };
  }, [lastRunId, selectedRunId, mode]);

  return { tasks, attempts, tasksRead, readFailed, clearRows };
}

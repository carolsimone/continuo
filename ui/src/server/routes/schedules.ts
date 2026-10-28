import { Router } from 'express';
import type { Response } from 'express';
import * as grpc from '@grpc/grpc-js';
import { GrpcClient, userMetadata } from '../clients/grpc-client';
import { executionOrderEdges } from './graph-edges';
import { grpcToHttpStatus, sendGrpcError } from './grpc-status';
import { parseOperation } from './operation';
import { parseLimit, parseOffset } from './paging';
import { requireRequestId } from './request-id';
import { toISO } from './timestamp';

const DEFAULT_RUNS_PAGE = 10;
// State clamps ListRuns page_size to this ceiling.
const MAX_RUNS_PAGE = 200;

// The code of a 409 that refuses a cron edit because another edit saved first.
const STALE_VERSION_CODE = 'stale_version';

// toSchedule renders the cron fields a RunService Schedule and
// ScheduleSummary share: version is int64, a string on the wire, and an unset
// time is null.
function toSchedule(s: any) {
  return {
    schedule_name: s.schedule_name,
    cron: s.cron ?? '',
    timezone: s.timezone ?? '',
    description: s.description ?? '',
    paused: Boolean(s.paused),
    next_fire_at: toISO(s.next_fire_at),
    version: Number(s.version ?? 0),
    updated_by: s.updated_by ?? '',
    updated_at: toISO(s.updated_at),
  };
}

// toScheduleSummary renders a RunService ScheduleSummary: its cron (empty,
// with version 0 and no times, when it has none), how many models of the
// live release carry its tag, and its last run.
function toScheduleSummary(s: any) {
  return {
    ...toSchedule(s),
    model_count: Number(s.model_count ?? 0),
    cron_words: s.cron_words ?? '',
    has_active_run: Boolean(s.has_active_run),
    last_run_at: toISO(s.last_run_at),
    last_run_status: s.last_run_status ?? '',
    last_run_id: s.last_run_id || null,
  };
}

function toISOList(times: any): string[] {
  return (times || []).map(toISO).filter((t: string | null): t is string => t !== null);
}

// grpcDetails is the text of a failed RunService call that the cron routes
// answer with: state's own message (the parser's, for a refused cron), which
// grpc-js keeps in details while its message prefixes it with the status
// code and name. An error raised before the call reached state has no
// details and keeps its own message.
function grpcDetails(err: any): string {
  return (typeof err?.details === 'string' && err.details) || err?.message || '';
}

// queryString reads a query parameter sent once; an absent or repeated one is
// empty.
function queryString(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

interface CronEdit {
  cron: string;
  timezone: string;
  description: string;
  paused: boolean;
  expected_version: number;
}

// parseCronEdit reads the body of PUT /:name/cron: cron and timezone are
// required strings, description a string and paused a boolean when sent, and
// expected_version a whole number of at least 0 (0, or absent, saves with no
// version check). A refusal is the message to answer with.
function parseCronEdit(body: any): CronEdit | string {
  const b = body ?? {};
  if (typeof b.cron !== 'string') return 'cron must be a string';
  if (typeof b.timezone !== 'string') return 'timezone must be a string';
  if (b.description !== undefined && typeof b.description !== 'string') return 'description must be a string';
  if (b.paused !== undefined && typeof b.paused !== 'boolean') return 'paused must be a boolean';
  const version = b.expected_version ?? 0;
  if (!Number.isSafeInteger(version) || version < 0) return 'expected_version must be a whole number of at least 0';
  return {
    cron: b.cron,
    timezone: b.timezone,
    description: b.description ?? '',
    paused: b.paused ?? false,
    expected_version: version,
  };
}

// createSchedulesRouter serves the schedules of the live release and their
// crons, their graphs and completed runs, starts a run of a schedule, and
// saves, removes and previews a schedule's cron, all through RunService.
export function createSchedulesRouter(client: GrpcClient) {
  const router = Router();

  // sendStaleVersion answers an edit refused because the schedule is no
  // longer at the version the editor read: 409 stale_version, naming who
  // made the latest edit. Aborted carries no editor, so the schedule is read
  // again; updated_by is null when its cron was removed meanwhile or the read
  // fails.
  const sendStaleVersion = (res: Response, name: string, err: any) => {
    client.listSchedules({}, (listErr: any, listResp: any) => {
      const current = listErr ? undefined : (listResp?.schedules || []).find((s: any) => s.schedule_name === name);
      const updatedBy = current?.cron ? current.updated_by || null : null;
      res.status(409).json({ error: grpcDetails(err), code: STALE_VERSION_CODE, updated_by: updatedBy });
    });
  };

  // GET /api/schedules — every schedule with its cron, its models and its last run
  router.get('/', (_req, res) => {
    client.listSchedules({}, (err: any, response: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      res.json({ schedules: (response.schedules || []).map(toScheduleSummary) });
    });
  });

  // GET /api/schedules/cron-preview?cron=&timezone= — a cron in words and its
  // next three fires, read by the parser that fires it; nothing is written.
  router.get('/cron-preview', (req, res) => {
    const request = { cron: queryString(req.query.cron), timezone: queryString(req.query.timezone) };
    client.previewSchedule(request, (err: any, response: any) => {
      if (err) return sendGrpcError(res, err, grpcDetails(err));
      res.json({ cron_words: response.cron_words ?? '', next_fire_times: toISOList(response.next_fire_times) });
    });
  });

  // GET /api/schedules/:name/graph — the live release's nodes of the schedule,
  // their one-hop upstreams in other schedules and the edges among them
  router.get('/:name/graph', (req, res) => {
    client.getScheduleGraph({ schedule_name: req.params.name }, (err: any, graphResp: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      const nodes = (graphResp.nodes || []).map((n: any) => ({
        node_id: `${n.service_name}.${n.schema_name}.${n.table_name}`,
        node_type: n.node_type,
        schedule_name: n.schedule_name,
      }));
      res.json({
        nodes,
        edges: executionOrderEdges(graphResp.edges),
        promotion_seq: Number(graphResp.promotion_seq ?? 0),
      });
    });
  });

  // GET /api/schedules/:name/runs?limit=&offset= — one page of a schedule's completed runs
  // (newest first), with the schedule's total completed run count.
  router.get('/:name/runs', (req, res) => {
    const request = {
      schedule_name: req.params.name,
      page_size: parseLimit(req.query.limit, { def: DEFAULT_RUNS_PAGE, max: MAX_RUNS_PAGE }),
      page_offset: parseOffset(req.query.offset),
    };
    client.listRuns(request, (err: any, resp: any) => {
      if (err) return res.status(grpcToHttpStatus(err.code)).json({ error: err.message });
      const runs = (resp.runs || []).map((r: any) => ({
        run_id: r.run_id,
        schedule_name: r.schedule_name,
        status: r.status,
        created_at: r.created_at || null,
        completed_at: r.completed_at || null,
        failure_reason: r.failure_reason || null,
      }));
      res.json({ runs, total_count: Number(resp.total_count ?? 0) });
    });
  });

  // POST /api/schedules/:name/trigger  {request_id, operation?} — starts a run
  // of the whole schedule. Cancelling is done by run id (POST /api/runs/:id/cancel).
  router.post('/:name/trigger', (req, res) => {
    const requestId = requireRequestId(req, res);
    if (requestId === null) return;
    const operation = parseOperation(req.body?.operation);
    if (operation === null) {
      return res.status(400).json({ error: 'operation must be one of "", run, test, build' });
    }
    client.startRun(
      { request_id: requestId, schedule_name: req.params.name, operation },
      userMetadata(req),
      (err: any, response: any) => {
        if (err) return sendGrpcError(res, err);
        res.json({ run_id: response.run_id });
      }
    );
  });

  // PUT /api/schedules/:name/cron  {cron, timezone, description?, paused?,
  // expected_version?} — creates or replaces the schedule's cron as an edit by
  // the caller, and answers with the schedule as saved and its next fires. A
  // refused cron or timezone is 400 with the parser's message, a first cron
  // for a name no model of the live release has is 404, and an edit made over
  // a newer one is 409 stale_version.
  router.put('/:name/cron', (req, res) => {
    const edit = parseCronEdit(req.body);
    if (typeof edit === 'string') return res.status(400).json({ error: edit });
    client.upsertSchedule(
      { schedule_name: req.params.name, ...edit },
      userMetadata(req),
      (err: any, response: any) => {
        if (err?.code === grpc.status.ABORTED) return sendStaleVersion(res, req.params.name, err);
        if (err) return sendGrpcError(res, err, grpcDetails(err));
        res.json({
          schedule: response.schedule ? toSchedule(response.schedule) : null,
          next_fire_times: toISOList(response.next_fire_times),
        });
      }
    );
  });

  // DELETE /api/schedules/:name/cron — removes the schedule's cron, which then
  // runs only when triggered; removing a cron the schedule does not have
  // succeeds.
  router.delete('/:name/cron', (req, res) => {
    client.deleteSchedule({ schedule_name: req.params.name }, userMetadata(req), (err: any) => {
      if (err) return sendGrpcError(res, err, grpcDetails(err));
      res.status(204).end();
    });
  });

  return router;
}

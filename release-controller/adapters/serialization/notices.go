package serialization

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// operationRun is the wire name of the run operation, whose domain value is empty.
const operationRun = "run"

// predictionRefused is the prediction of a notice whose load no command can apply.
const predictionRefused = "refused"

// NoticeDTO is the JSON shape of one release.Notice, as it is stored in the
// release_pipeline_runs.notices JSONB column and returned in the notices field
// of GET /releases/{id}. Every list is always an array, never null. A rebuild
// carries no reason and no relation, so a notice read back has neither.
type NoticeDTO struct {
	Kind              string                `json:"kind"`
	NodeID            string                `json:"node_id"`
	Service           string                `json:"service"`
	EngineFamily      string                `json:"engine_family"`
	Change            string                `json:"change"`
	Columns           ColumnChangesDTO      `json:"columns"`
	Load              SeedLoadDTO           `json:"load"`
	Rebuilds          []RebuildDTO          `json:"rebuilds"`
	RefreshOnSchedule []ScheduledRefreshDTO `json:"refresh_on_schedule"`
	Warehouse         WarehouseFindingsDTO  `json:"warehouse"`
	Prediction        string                `json:"prediction"`
	StopReason        string                `json:"stop_reason"`
	Unvalidated       bool                  `json:"unvalidated"`
}

// ColumnChangesDTO is the JSON shape of release.ColumnChanges.
type ColumnChangesDTO struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Retyped []string `json:"retyped"`
}

// SeedLoadDTO is the JSON shape of the seed's own load.
type SeedLoadDTO struct {
	Operation  string `json:"operation"`
	DropsTable bool   `json:"drops_table"`
	Reason     string `json:"reason"`
}

// RebuildDTO is the JSON shape of one view the run rebuilds.
type RebuildDTO struct {
	NodeID       string `json:"node_id"`
	Materialized string `json:"materialized"`
	Operation    string `json:"operation"`
	Schedule     string `json:"schedule"`
}

// ScheduledRefreshDTO is the JSON shape of one node below the seed that
// refreshes on its own schedule.
type ScheduledRefreshDTO struct {
	NodeID       string `json:"node_id"`
	Materialized string `json:"materialized"`
	Schedule     string `json:"schedule"`
}

// WarehouseFindingsDTO is the JSON shape of release.WarehouseFindings.
type WarehouseFindingsDTO struct {
	Checked         bool               `json:"checked"`
	Completeness    string             `json:"completeness"`
	OutsideContinuo []OutsideObjectDTO `json:"outside_continuo"`
}

// OutsideObjectDTO is the JSON shape of release.OutsideObject.
type OutsideObjectDTO struct {
	Object string `json:"object"`
	Kind   string `json:"kind"`
	Owner  string `json:"owner"`
}

// NoticesFromDomain maps notices to DTOs. The result is never nil, so a release
// with no notice serialises as [].
func NoticesFromDomain(in []release.Notice) []NoticeDTO {
	out := make([]NoticeDTO, 0, len(in))
	for _, n := range in {
		rebuilds := make([]RebuildDTO, 0, len(n.Rebuilds))
		for _, r := range n.Rebuilds {
			rebuilds = append(rebuilds, RebuildDTO{NodeID: r.Key, Materialized: r.Materialized, Operation: operationToWire(r.Operation), Schedule: r.Schedule})
		}
		refresh := make([]ScheduledRefreshDTO, 0, len(n.RefreshOnSchedule))
		for _, b := range n.RefreshOnSchedule {
			refresh = append(refresh, ScheduledRefreshDTO{NodeID: b.Key, Materialized: b.Materialized, Schedule: b.Schedule})
		}
		outside := make([]OutsideObjectDTO, 0, len(n.Warehouse.OutsideContinuo))
		for _, o := range n.Warehouse.OutsideContinuo {
			outside = append(outside, OutsideObjectDTO(o))
		}
		out = append(out, NoticeDTO{
			Kind:         n.Kind,
			NodeID:       n.NodeID,
			Service:      n.Service,
			EngineFamily: string(n.EngineFamily),
			Change:       string(n.Change),
			Columns: ColumnChangesDTO{
				Added:   nonNil(n.Columns.Added),
				Removed: nonNil(n.Columns.Removed),
				Retyped: nonNil(n.Columns.Retyped),
			},
			Load:              SeedLoadDTO{Operation: operationToWire(n.Load.Operation), DropsTable: n.Load.DropsTable, Reason: n.Load.Reason},
			Rebuilds:          rebuilds,
			RefreshOnSchedule: refresh,
			Warehouse:         WarehouseFindingsDTO{Checked: n.Warehouse.Checked, Completeness: n.Warehouse.Completeness, OutsideContinuo: outside},
			Prediction:        n.Prediction,
			StopReason:        n.StopReason,
			Unvalidated:       n.Unvalidated,
		})
	}
	return out
}

// NoticesToDomain maps decoded DTOs back to notices. A refused notice's stop
// reason is its load's refusal. The result is never nil.
func NoticesToDomain(in []NoticeDTO) []release.Notice {
	out := make([]release.Notice, 0, len(in))
	for _, d := range in {
		rebuilds := make([]seedreload.Rebuild, 0, len(d.Rebuilds))
		for _, r := range d.Rebuilds {
			rebuilds = append(rebuilds, seedreload.Rebuild{Key: r.NodeID, Materialized: r.Materialized, Operation: operationFromWire(r.Operation), Schedule: r.Schedule})
		}
		refresh := make([]seedreload.Boundary, 0, len(d.RefreshOnSchedule))
		for _, b := range d.RefreshOnSchedule {
			refresh = append(refresh, seedreload.Boundary{Key: b.NodeID, Materialized: b.Materialized, Schedule: b.Schedule})
		}
		outside := make([]release.OutsideObject, 0, len(d.Warehouse.OutsideContinuo))
		for _, o := range d.Warehouse.OutsideContinuo {
			outside = append(outside, release.OutsideObject(o))
		}
		load := seedreload.Load{Operation: operationFromWire(d.Load.Operation), DropsTable: d.Load.DropsTable, Reason: d.Load.Reason}
		if d.Prediction == predictionRefused {
			load.Refusal = pkg_model.FailureReason(d.StopReason)
		}
		out = append(out, release.Notice{
			Kind:         d.Kind,
			NodeID:       d.NodeID,
			Service:      d.Service,
			EngineFamily: warehouse.Family(d.EngineFamily),
			Change:       seedreload.Change(d.Change),
			Columns: release.ColumnChanges{
				Added:   nonNil(d.Columns.Added),
				Removed: nonNil(d.Columns.Removed),
				Retyped: nonNil(d.Columns.Retyped),
			},
			Load:              load,
			Rebuilds:          rebuilds,
			RefreshOnSchedule: refresh,
			Warehouse:         release.WarehouseFindings{Checked: d.Warehouse.Checked, Completeness: d.Warehouse.Completeness, OutsideContinuo: outside},
			Prediction:        d.Prediction,
			StopReason:        d.StopReason,
			Unvalidated:       d.Unvalidated,
		})
	}
	return out
}

func operationToWire(op pkg_model.Operation) string {
	if op == pkg_model.OperationRun {
		return operationRun
	}
	return string(op)
}

func operationFromWire(op string) pkg_model.Operation {
	if op == operationRun {
		return pkg_model.OperationRun
	}
	return pkg_model.Operation(op)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

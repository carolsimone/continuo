package domain

import (
	"time"
)

type Criticality string

const (
	CriticalityUnspecified Criticality = "UNSPECIFIED"
	CriticalityRegulatory  Criticality = "REGULATORY"
	CriticalityCore        Criticality = "CORE"
	CriticalitySecondary   Criticality = "SECONDARY"
)

type GraphEdge struct {
	FromNodeID string // "{service_name}.{schema_name}.{table_name}"
	ToNodeID   string
}

type TableNode struct {
	TableName     string
	SchemaName    string
	ServiceName   string
	Owner         string
	ScheduleName  string
	Criticality   Criticality
	LastUpdatedAt time.Time
	CreatedAt     time.Time
	NodeType      string
	Status        string
	TaskID        string
	ImageTag      string
}

type ScheduleGraph struct {
	Nodes        []*TableNode
	Edges        []*GraphEdge
	PromotionSeq int64 // :TopologyRoot.promotion_seq (the live promotion seq) at query time; 0 when unknown.
}

// EventTypeReleasePromoted is the event_type of the orchestrator_outbox row the
// topology swap writes for schedules.loaded:v1. The emit site
// (service/handlers) and the publisher's Render switch (adapters/publisher)
// share this one declaration. The value is the stored event_type and must not
// change.
const EventTypeReleasePromoted = "release_promoted"

package run

import "github.com/carolsimone/continuo/orchestrator/domain"

// ScheduleInitNodes carries the three node sets used by initial-dispatch:
// AllNodes (every node in scope), RootNodes (no upstream), and SeedNodes
// (upstream-seeded entry points).
type ScheduleInitNodes struct {
	AllNodes  []*domain.TableNode
	RootNodes []*domain.TableNode
	SeedNodes []*domain.TableNode
}

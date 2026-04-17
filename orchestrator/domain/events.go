package domain

type TopologyIngested struct {
	ScheduleNames    []string
	ManifestVersions map[string]string
}

type RunInitialized struct {
	RunID     string
	AllNodes  []*TableNode
	RootNodes []*TableNode
	SeedNodes []*TableNode
}

type RerunReady struct {
	RunID       string
	TargetNodes []*TableNode
}

type NodeCompleted struct {
	RunID           string
	ScheduleName    string
	SchemaName      string
	TableName       string
	Status          NodeStatus
	ReadyDownstream []*DownstreamInfo
}

type DownstreamInfo struct {
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    string
}

type RunCompleted struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string
}

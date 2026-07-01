package streams

// All is every continuo event stream name. The stream reaper trims exactly
// these keys by age; anything not listed here is never touched — in particular
// the ui-service auth session keys, which are plain Redis strings (not streams)
// and share the same Redis instance.
//
// Hand-maintained: TestAllMatchesContract guards it against drift from
// contract.yaml, so a stream added to the contract fails CI until it is added
// here too.
var All = []string{
	SchedulerStartedV1,
	SchedulesLoadedV1,
	RunEntriesDispatchedV1,
	RunEntriesDispatchFailedV1,
	TaskStatusUpdatedV1,
	TaskExecutionRecordedV1,
	NodeUpdatedV1,
	TriggerRerunV1,
	TriggerRebaseV1,
	TriggerSingleNodeRunV1,
	RunFinalizedV1,
	QueryModelV1,
	RetryTaskV1,
	NodeDeployedV1,
	CheckK8sV1,
	TaskFailedV1,
	ScheduleCancelledV1,
	ReleaseRequestedV1,
	ManifestLoadedCandidateV1,
	ValidationRequestedV1,
	ValidationNodeCompletedV1,
	ValidationCompletedV1,
	SeedBuildRequestedV1,
	SeedBuildNodeCompletedV1,
	SeedBuildCompletedV1,
	CompileRequestedV1,
	CompileNodeCompletedV1,
	CompileCompletedV1,
	ReleasePromotedV1,
	ReleaseRejectedV1,
	RemediationRequestedV1,
	RemediationProposedV1,
	RemediationPrOpenedV1,
}

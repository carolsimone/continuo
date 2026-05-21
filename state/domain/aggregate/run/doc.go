// Package run hosts the Run aggregate root, its child Task entity, the value
// objects that describe scheduler/task lifecycle (statuses, kinds, caller
// identities, service metadata), the outbound domain events the aggregate
// records during mutation, and the TaskCollection port the aggregate calls
// to look up or mutate child task state without loading the full collection.
//
// All invariants over a scheduler run live here. The application layer in
// state/service/handlers loads a Run via repository.RunRepository.LoadRunForUpdate,
// invokes one method, then calls repository.RunRepository.SaveRun and
// ports.OutboxPublisher.Append. The aggregate never imports infrastructure.
package run

package events

// DefaultTaskMaxRetries is the retry budget of every task state plans. state
// stores it as each task's max_retries and stamps it on the task's
// query.model:v1 dispatch, where execution-controller retries a failed attempt
// while its retry count is below it, so both read one value.
// execution-controller applies it itself only to a dispatch that carries no
// budget.
const DefaultTaskMaxRetries int32 = 2

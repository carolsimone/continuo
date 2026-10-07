package model

// ScopeGlobal names the capacity record every deployment is admitted under.
const ScopeGlobal = "global"

// Headroom returns how many deployments may be reserved now: the slots free
// under limit while inFlight are held, at most batch, and never below zero (a
// limit lowered below what is already in flight admits nothing until enough
// deployments finish).
func Headroom(limit, inFlight, batch int) int {
	free := limit - inFlight
	if free > batch {
		free = batch
	}
	if free < 0 {
		return 0
	}
	return free
}

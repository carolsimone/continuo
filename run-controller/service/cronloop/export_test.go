package cronloop

import "time"

// SetFireTimeout bounds each fire of l by d instead of FireTimeout, so a test
// sees a fire run out of time without waiting FireTimeout.
func SetFireTimeout(l *Loop, d time.Duration) { l.fireTimeout = d }

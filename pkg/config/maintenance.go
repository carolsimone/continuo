package config

import (
	"fmt"
	"os"
)

// MaintenanceEnvKey names the environment variable that switches maintenance
// mode on. Every service that accepts new work requires it.
const MaintenanceEnvKey = "MAINTENANCE_ENABLED"

// LoadMaintenance reads MAINTENANCE_ENABLED, which must be exactly "true" or
// "false". Unset, empty, or any other spelling is recorded on v, so the service
// refuses to start instead of guessing whether it may accept new work.
func LoadMaintenance(v *Validator) bool {
	raw, set := os.LookupEnv(MaintenanceEnvKey)
	switch {
	case raw == "true":
		return true
	case raw == "false":
		return false
	case !set || raw == "":
		v.Add(MaintenanceEnvKey + ` (required: "true" or "false")`)
	default:
		v.Add(fmt.Sprintf(`%s (invalid value %q: expected exactly "true" or "false")`, MaintenanceEnvKey, raw))
	}
	return false
}

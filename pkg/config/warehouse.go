package config

import (
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/pkg/warehouse"
)

// LoadWarehouse reads WAREHOUSE_ENGINE, defaulting an unset value to postgres,
// and returns the engine with its drop family. An engine with no family records
// a validation failure on v and returns the empty family, so the service stops
// at boot rather than running with the wrong drop rules.
func LoadWarehouse(v *Validator) (string, warehouse.Family) {
	engine := EnvOrDefault("WAREHOUSE_ENGINE", "")
	family, err := warehouse.FamilyOf(engine)
	if err != nil {
		v.Add(fmt.Sprintf("WAREHOUSE_ENGINE (unsupported %q: expected one of %s)",
			engine, strings.Join(warehouse.Engines(), ", ")))
		return engine, ""
	}
	if engine == "" {
		engine = "postgres"
	}
	return engine, family
}

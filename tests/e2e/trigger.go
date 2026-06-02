package e2e

import "fmt"

const (
	testScheduleName        = "e2e-schedule"
	testSchemaName          = "e2e_schema"
	testOwner               = "test"
	failureTestScheduleName = "e2e-schedule-failure"
)

// tableServiceMap maps each happy-path table to its owning service
var tableServiceMap = map[string]string{
	"seed_table_1": "service-1",
	"seed_table_2": "service-1",
	"seed_table_3": "service-1",
	"table_a":      "service-1",
	"table_b":      "service-1",
	"table_c":      "service-1",
	"table_d":      "service-3",
	"table_e":      "service-3",
	"table_f":      "service-3",
	"table_g":      "service-2",
	"table_h":      "service-2",
	"table_i":      "service-3",
	"table_j":      "service-3",
}

// getServiceNameForTable returns the service name for a happy-path table
func getServiceNameForTable(tableName string) string {
	svc, ok := tableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}

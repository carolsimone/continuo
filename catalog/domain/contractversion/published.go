package contractversion

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrUnsupportedEngine reports a warehouse engine with no server type.
var ErrUnsupportedEngine = errors.New("contractversion: unsupported warehouse engine")

// Server is the warehouse entry of a published contract's `servers` list, built
// from install settings. A Server with no Host stands for "no servers entry".
type Server struct {
	// Type is the ODCS server type: postgres or trino.
	Type string
	Host string
	Port int
	// Database is the database name, or the catalog name when Type is trino.
	Database string
}

// ServerType maps a WAREHOUSE_ENGINE value to its ODCS server type: empty and
// postgres are postgres, trino is trino. Any other engine wraps
// ErrUnsupportedEngine.
func ServerType(engine string) (string, error) {
	switch engine {
	case "", "postgres":
		return "postgres", nil
	case "trino":
		return "trino", nil
	default:
		return "", fmt.Errorf("%w %q: expected postgres or trino", ErrUnsupportedEngine, engine)
	}
}

// Fingerprint identifies the published output of the settings: sum is applied
// to a canonical text of every setting a published file shows, so two Servers
// with equal fingerprints publish identical servers entries. A Server with no
// Host publishes no entry, so its other settings do not count.
func (s Server) Fingerprint(sum func([]byte) string) string {
	if s.Host == "" {
		return sum([]byte("server=none\n"))
	}
	return sum(fmt.Appendf(nil, "type=%q\nhost=%q\nport=%d\ndatabase=%q\n", s.Type, s.Host, s.Port, s.Database))
}

// entry is the server's `servers` list entry for a contract in schema. The
// database key is `catalog` for trino. An empty database or schema is left out.
func (s Server) entry(schema string) map[string]any {
	e := map[string]any{
		"server": "warehouse",
		"type":   s.Type,
		"host":   s.Host,
		"port":   s.Port,
	}
	databaseKey := "database"
	if s.Type == "trino" {
		databaseKey = "catalog"
	}
	if s.Database != "" {
		e[databaseKey] = s.Database
	}
	if schema != "" {
		e["schema"] = schema
	}
	return e
}

// Provenance is the facts of the promotion that made a contract version
// current, which the published form carries beside the stored document.
type Provenance struct {
	ContractHash      string
	ContentHash       string
	ReleaseID         string
	ContinuoContracts string
	SourcePath        string
	PromotionSeq      int64
	PromotedAt        time.Time
}

// value is the provenance custom property's value. promotedAt is RFC 3339 UTC.
func (p Provenance) value() map[string]any {
	return map[string]any{
		"contractHash":      p.ContractHash,
		"contentHash":       p.ContentHash,
		"releaseId":         p.ReleaseID,
		"promotionSeq":      p.PromotionSeq,
		"promotedAt":        p.PromotedAt.UTC().Format(time.RFC3339),
		"continuoContracts": p.ContinuoContracts,
		"sourcePath":        p.SourcePath,
	}
}

// ServerSchema is the schema of a published contract's `servers` entry:
// declared, the schema the node is declared in and builds in, when it is not
// empty, and otherwise the part of stored's id before its first dot ("" when
// the id has no dot), so a versioned dbt model's `schema.name.vN` keeps its
// schema. A contract id is lower case while a dbt schema can be quoted with
// capitals, so only the declared spelling names the schema the warehouse holds.
func ServerSchema(stored map[string]any, declared string) string {
	if declared != "" {
		return declared
	}
	id, _ := stored["id"].(string)
	schema, _, found := strings.Cut(id, ".")
	if !found {
		return ""
	}
	return schema
}

// Published returns the published form of a stored contract: a copy whose
// `version` is the node's counter written as a string, whose `servers` list
// holds the install's warehouse entry when server has a host, and whose
// `customProperties` end with the continuo provenance property (any provenance
// the stored form carried is replaced). The schema of a servers entry is
// ServerSchema(stored, schema), where schema is the node's declared schema
// ("" when it has none); the contract id is not changed. stored is not
// changed, and the result shares nothing with it.
func Published(stored map[string]any, version int64, server *Server, schema string, prov Provenance) map[string]any {
	out := cloneObject(stored)
	out["version"] = strconv.FormatInt(version, 10)

	if server != nil && server.Host != "" {
		out["servers"] = []any{server.entry(ServerSchema(stored, schema))}
	}

	props, _ := out["customProperties"].([]any)
	kept := make([]any, 0, len(props)+1)
	for _, p := range props {
		if !isProvenanceProperty(p) {
			kept = append(kept, p)
		}
	}
	out["customProperties"] = append(kept, map[string]any{
		"vendor":   "continuo",
		"property": "provenance",
		"value":    prov.value(),
	})
	return out
}

// IsEphemeral reports whether a stored contract describes an ephemeral model:
// its first schema object's physicalType is `ephemeral`. An ephemeral model
// creates no table, so it has no published file.
func IsEphemeral(stored map[string]any) bool {
	schema, _ := stored["schema"].([]any)
	if len(schema) == 0 {
		return false
	}
	first, _ := schema[0].(map[string]any)
	physicalType, _ := first["physicalType"].(string)
	return physicalType == "ephemeral"
}

func isProvenanceProperty(p any) bool {
	prop, ok := p.(map[string]any)
	return ok && prop["vendor"] == "continuo" && prop["property"] == "provenance"
}

func cloneObject(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+3)
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneObject(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	default:
		return v
	}
}

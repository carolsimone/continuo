package deploycheck

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// helperJSON returns the JSON body of a one-line named template in the
// chart's _helpers.tpl, e.g. continuo.redis.clientServices.
func helperJSON(t *testing.T, name string, into any) {
	t.Helper()
	helpers := readRepoFile(t, "deploy", "continuo", "templates", "_helpers.tpl")
	re := regexp.MustCompile(`(?s)\{\{- define "` + regexp.QuoteMeta(name) + `" -\}\}\s*(.*?)\s*\{\{- end -\}\}`)
	m := re.FindStringSubmatch(helpers)
	if m == nil {
		t.Fatalf("_helpers.tpl defines no %s", name)
	}
	if err := json.Unmarshal([]byte(m[1]), into); err != nil {
		t.Fatalf("_helpers.tpl %s is not JSON: %v", name, err)
	}
}

// guideLoginTable returns the rows of the login table in deploy/CREDENTIALS.md,
// each as its cells.
func guideLoginTable(t *testing.T) [][]string {
	t.Helper()
	guide := readRepoFile(t, "deploy", "CREDENTIALS.md")
	start := strings.Index(guide, "## Every login")
	if start < 0 {
		t.Fatal("deploy/CREDENTIALS.md has no `## Every login` section")
	}
	section := guide[start+len("## Every login"):]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	var rows [][]string
	for _, line := range strings.Split(section, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || strings.HasPrefix(line, "|---") || strings.HasPrefix(line, "| ---") {
			continue
		}
		var cells []string
		for _, c := range strings.Split(strings.Trim(line, "|"), "|") {
			cells = append(cells, strings.TrimSpace(c))
		}
		rows = append(rows, cells)
	}
	if len(rows) < 2 {
		t.Fatal("deploy/CREDENTIALS.md's `## Every login` section has no table")
	}
	return rows[1:]
}

// guideHasLogin reports whether the guide's login table has a row for login
// on datastore (Postgres or Redis).
func guideHasLogin(rows [][]string, datastore, login string) bool {
	for _, row := range rows {
		if len(row) >= 2 && row[0] == datastore && row[1] == "`"+login+"`" {
			return true
		}
	}
	return false
}

type loginValues struct {
	Credentials struct {
		Postgres map[string]struct {
			Username string `yaml:"username"`
		} `yaml:"postgres"`
		Redis struct {
			Logins map[string]struct {
				Username string `yaml:"username"`
			} `yaml:"logins"`
		} `yaml:"redis"`
	} `yaml:"credentials"`
	Services []struct {
		Name string            `yaml:"name"`
		Env  map[string]string `yaml:"env"`
	} `yaml:"services"`
}

func readLoginValues(t *testing.T) loginValues {
	t.Helper()
	var v loginValues
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.yaml")), &v); err != nil {
		t.Fatalf("parse values.yaml: %v", err)
	}
	return v
}

// TestEveryPostgresDatabaseHasItsLogin: each database db/migrate-all.sh
// migrates has a service that keeps it, a login in credentials.postgres, an
// entry in the chart's continuo.postgres.serviceDatabases (from which the
// migrate Job's CONTINUO_PG_LOGINS is built), a CREATE ROLE in the compose
// init script and a row in the credentials guide; and so has the warehouse
// login. A database added without a login would stop every deploy at the
// migrate Job, and a login the guide does not name is one no company creates.
func TestEveryPostgresDatabaseHasItsLogin(t *testing.T) {
	values := readLoginValues(t)
	var serviceDatabases map[string]string
	helperJSON(t, "continuo.postgres.serviceDatabases", &serviceDatabases)
	initScript := readRepoFile(t, "db", "init-databases.sh")
	rows := guideLoginTable(t)

	dbs := databasesAssignment(t, readRepoFile(t, "db", "migrate-all.sh"), "db/migrate-all.sh")
	var mapped []string
	for _, db := range serviceDatabases {
		mapped = append(mapped, db)
	}
	assertSameSet(t, "_helpers.tpl continuo.postgres.serviceDatabases", mapped, dbs)

	for _, db := range dbs {
		var svc string
		for _, s := range values.Services {
			if s.Env["POSTGRES_DB"] == "continuo_"+db {
				svc = s.Name
			}
		}
		if svc == "" {
			t.Errorf("continuo_%s: no service in values.yaml keeps it (env.POSTGRES_DB)", db)
			continue
		}
		if serviceDatabases[svc] != db {
			t.Errorf("continuo_%s: _helpers.tpl continuo.postgres.serviceDatabases maps %s to %q", db, svc, serviceDatabases[svc])
		}
		login := values.Credentials.Postgres[svc].Username
		if login == "" {
			t.Errorf("continuo_%s: credentials.postgres.%s names no login", db, svc)
			continue
		}
		if !strings.Contains(initScript, "CREATE ROLE "+login+" LOGIN") {
			t.Errorf("continuo_%s: db/init-databases.sh does not CREATE ROLE %s", db, login)
		}
		if !guideHasLogin(rows, "Postgres", login) {
			t.Errorf("continuo_%s: deploy/CREDENTIALS.md's login table has no Postgres row for %s", db, login)
		}
	}

	warehouse := values.Credentials.Postgres["warehouse"].Username
	if warehouse == "" {
		t.Fatal("credentials.postgres.warehouse names no login")
	}
	if !strings.Contains(initScript, "CREATE ROLE "+warehouse+" LOGIN") {
		t.Errorf("db/init-databases.sh does not CREATE ROLE %s", warehouse)
	}
	if !guideHasLogin(rows, "Postgres", warehouse) {
		t.Errorf("deploy/CREDENTIALS.md's login table has no Postgres row for %s", warehouse)
	}
}

// TestEveryRedisServiceHasItsLogin: each service the chart treats as a Redis
// client, and streams-init, has a login in credentials.redis.logins whose
// username is the one the generated rules give it, and a row in the guide;
// the rules name no login the chart does not use.
func TestEveryRedisServiceHasItsLogin(t *testing.T) {
	values := readLoginValues(t)
	var clients []string
	helperJSON(t, "continuo.redis.clientServices", &clients)
	var rules struct {
		Logins []struct {
			Service  string `json:"service"`
			Username string `json:"username"`
		} `json:"logins"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "files", "redis-acl-rules.json")), &rules); err != nil {
		t.Fatalf("parse redis-acl-rules.json: %v", err)
	}
	ruled := map[string]string{}
	for _, l := range rules.Logins {
		ruled[l.Service] = l.Username
	}
	rows := guideLoginTable(t)

	names := append([]string{"streams-init"}, clients...)
	var ruledNames []string
	for name := range ruled {
		ruledNames = append(ruledNames, name)
	}
	sort.Strings(ruledNames)
	assertSameSet(t, "redis-acl-rules.json logins", ruledNames, names)

	for _, name := range names {
		login := values.Credentials.Redis.Logins[name].Username
		if login == "" {
			t.Errorf("%s: credentials.redis.logins.%s names no login", name, name)
			continue
		}
		if ruled[name] != login {
			t.Errorf("%s: credentials.redis.logins names %q, the generated rules %q", name, login, ruled[name])
		}
		if !guideHasLogin(rows, "Redis", login) {
			t.Errorf("%s: deploy/CREDENTIALS.md's login table has no Redis row for %s", name, login)
		}
	}
}

// TestGuideNamesOnlyLoginsTheChartUses: every row of the guide's login table
// names a login the chart gives a service, streams-init, the warehouse or the
// migration step.
func TestGuideNamesOnlyLoginsTheChartUses(t *testing.T) {
	values := readLoginValues(t)
	known := map[string]bool{}
	for _, l := range values.Credentials.Postgres {
		known["Postgres "+l.Username] = true
	}
	for _, l := range values.Credentials.Redis.Logins {
		known["Redis "+l.Username] = true
	}
	for _, row := range guideLoginTable(t) {
		if len(row) < 2 {
			continue
		}
		login := strings.Trim(row[1], "`")
		if row[0] == "Postgres" && strings.Contains(row[1], "externalDatabase.username") {
			continue
		}
		if !known[row[0]+" "+login] {
			t.Errorf("deploy/CREDENTIALS.md's login table names %s login %s, which the chart does not use", row[0], row[1])
		}
	}
}

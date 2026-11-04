package deploycheck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type chartValues struct {
	Credentials struct {
		Postgres map[string]struct {
			Username string `yaml:"username"`
		} `yaml:"postgres"`
	} `yaml:"credentials"`
	Validation struct {
		WarehouseDatabase string `yaml:"warehouseDatabase"`
	} `yaml:"validation"`
	Services []struct {
		Name string            `yaml:"name"`
		Env  map[string]string `yaml:"env"`
	} `yaml:"services"`
}

type redisRules struct {
	Logins []struct {
		Service  string `yaml:"service" json:"service"`
		Username string `yaml:"username" json:"username"`
	} `json:"logins"`
}

// composeRedisService maps a docker-compose service that connects to Redis to
// the service whose generated Redis login it signs in as.
var composeRedisService = map[string]string{"ui-auth": "ui"}

// composeRedisExempt lists the docker-compose services that sign in to Redis
// as its default user: the e2e harness, which reads and writes every stream.
var composeRedisExempt = map[string]bool{"e2e": true}

// TestComposeServicesUseTheirOwnLogins holds docker-compose to the logins the
// chart gives each service: a service that keeps a Postgres database signs in
// as the login credentials.postgres names for it (the warehouse login for the
// dbt warehouse), and a service that connects to Redis signs in as its
// generated Redis login, each with the local password local-<login>, and with
// REDIS_ACL_MODE=perService. The one migrations service receives every
// database's login.
func TestComposeServicesUseTheirOwnLogins(t *testing.T) {
	var compose composeFile
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "docker-compose.yml")), &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}
	var values chartValues
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.yaml")), &values); err != nil {
		t.Fatalf("parse values.yaml: %v", err)
	}
	var rules redisRules
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "files", "redis-acl-rules.json")), &rules); err != nil {
		t.Fatalf("parse redis-acl-rules.json: %v", err)
	}

	pgLoginByDB := map[string]string{}
	for _, svc := range values.Services {
		db := svc.Env["POSTGRES_DB"]
		if db == "" {
			continue
		}
		login := values.Credentials.Postgres[svc.Name].Username
		if login == "" {
			t.Errorf("values.yaml: service %s keeps %s but credentials.postgres names no login for it", svc.Name, db)
		}
		pgLoginByDB[db] = login
	}
	if wh := values.Credentials.Postgres["warehouse"].Username; wh == "" {
		t.Error("values.yaml: credentials.postgres.warehouse names no login")
	} else {
		pgLoginByDB[values.Validation.WarehouseDatabase] = wh
	}
	redisLogin := map[string]string{}
	for _, l := range rules.Logins {
		redisLogin[l.Service] = l.Username
	}

	names := make([]string, 0, len(compose.Services))
	for name := range compose.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		env := compose.Services[name].Environment
		if db := env["POSTGRES_DB"]; db != "" {
			want, ok := pgLoginByDB[db]
			if !ok {
				t.Errorf("docker-compose.yml %s: POSTGRES_DB=%s is no database the chart gives a login", name, db)
			} else {
				if env["POSTGRES_USER"] != want {
					t.Errorf("docker-compose.yml %s: POSTGRES_USER=%q, want %q", name, env["POSTGRES_USER"], want)
				}
				if env["POSTGRES_PASSWORD"] != "local-"+want {
					t.Errorf("docker-compose.yml %s: POSTGRES_PASSWORD=%q, want %q", name, env["POSTGRES_PASSWORD"], "local-"+want)
				}
			}
		}
		_, addr := env["REDIS_ADDR"]
		_, host := env["REDIS_HOST"]
		_, url := env["REDIS_URL"]
		if (addr || host || url) && !composeRedisExempt[name] {
			svc := name
			if mapped, ok := composeRedisService[name]; ok {
				svc = mapped
			}
			want, ok := redisLogin[svc]
			if !ok {
				t.Errorf("docker-compose.yml %s connects to Redis but redis-acl-rules.json has no login for %s", name, svc)
				continue
			}
			if env["REDIS_USERNAME"] != want {
				t.Errorf("docker-compose.yml %s: REDIS_USERNAME=%q, want %q", name, env["REDIS_USERNAME"], want)
			}
			if env["REDIS_PASSWORD"] != "local-"+want {
				t.Errorf("docker-compose.yml %s: REDIS_PASSWORD=%q, want %q", name, env["REDIS_PASSWORD"], "local-"+want)
			}
			if env["REDIS_ACL_MODE"] != "perService" {
				t.Errorf("docker-compose.yml %s: REDIS_ACL_MODE=%q, want perService", name, env["REDIS_ACL_MODE"])
			}
		}
	}

	migrations, ok := compose.Services["migrations"]
	if !ok {
		t.Fatal("docker-compose.yml has no migrations service")
	}
	env := migrations.Environment
	got := strings.Fields(env["CONTINUO_PG_LOGINS"])
	var want []string
	for db, login := range pgLoginByDB {
		if db == values.Validation.WarehouseDatabase {
			continue
		}
		want = append(want, strings.TrimPrefix(db, "continuo_")+"="+login)
	}
	assertSameSet(t, "docker-compose.yml migrations CONTINUO_PG_LOGINS", got, want)
}

// TestE2EHarnessReadsNoServiceCredential: the e2e harness runs inside the
// catalog container, whose REDIS_* and POSTGRES_* variables are catalog's own
// login. The harness signs in as the migration login and Redis's default user,
// so it reads its credentials from E2E_* variables and never from the
// service's.
func TestE2EHarnessReadsNoServiceCredential(t *testing.T) {
	root := repoRoot(t)
	files, err := filepath.Glob(filepath.Join(root, "tests", "e2e", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	read := regexp.MustCompile(`getEnv\("(REDIS_PASSWORD|REDIS_USERNAME|POSTGRES_PASSWORD|POSTGRES_USER)"`)
	for _, f := range files {
		raw, err := os.ReadFile(f) //nolint:gosec // G304: f is a glob match under repoRoot(t), not external input
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range read.FindAllStringSubmatch(string(raw), -1) {
			t.Errorf("%s reads %s, which inside the catalog container is catalog's own login: read an E2E_ variable instead", filepath.Base(f), m[1])
		}
	}
}

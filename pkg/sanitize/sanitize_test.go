package sanitize_test

import (
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/sanitize"
	"github.com/stretchr/testify/assert"
)

// pemKey is a private key block as a log or a source file carries it.
const pemKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
	"MIIEowIBAAKCAQEAxJ5g1c0Zk3lQ\n" +
	"0b9vT2hQ7WZkq1c\n" +
	"-----END RSA PRIVATE KEY-----"

const (
	awsKeyID  = "AKIAIOSFODNN7EXAMPLE" //nolint:gosec // G101: the example key id AWS documents, not a credential.
	githubPAT = "ghp_0123456789abcdefghijklmnopqrstuvwxyzAB"
	jwt       = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r"
)

func TestText_PreservesContentByteForByte(t *testing.T) {
	in := "select 1 as x -- a comment\nfrom \"analytics\".orders\n"
	assert.Equal(t, in, sanitize.Text(in))
}

func TestText_EmptyStringIsEmpty(t *testing.T) {
	assert.Equal(t, "", sanitize.Text(""))
}

// Rule 1: a private key block goes, from its BEGIN line to its END line.
func TestText_RedactsAPrivateKeyBlock(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"the block alone": {pemKey, "[redacted:key]"},
		"the block between two lines": {
			"before\n" + pemKey + "\nafter",
			"before\n[redacted:key]\nafter",
		},
		"a PKCS8 block": {
			"-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----",
			"[redacted:key]",
		},
		"an OpenSSH block": {
			"x -----BEGIN OPENSSH PRIVATE KEY-----\nb3Bl\n-----END OPENSSH PRIVATE KEY----- y",
			"x [redacted:key] y",
		},
		"two blocks keep the text between them": {
			pemKey + "\nmiddle\n" + pemKey,
			"[redacted:key]\nmiddle\n[redacted:key]",
		},
		"a block a log escaped onto one line": {
			`{"msg":"-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n","level":"info"}`,
			`{"msg":"[redacted:key]\n","level":"info"}`,
		},
		// An excerpt cut mid-key has no END line: everything after BEGIN goes.
		"a block cut before its END line": {
			"error near\n-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkq",
			"error near\n[redacted:key]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

func TestText_KeepsPublicKeyMaterial(t *testing.T) {
	for name, in := range map[string]string{
		"a certificate": "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----",
		"a public key":  "-----BEGIN PUBLIC KEY-----\nMFkw\n-----END PUBLIC KEY-----",
		"a mention":     "the PRIVATE KEY header is written on its own line",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// Rule 2: only the password of a URL goes. The scheme, user, host and path stay.
func TestText_RedactsThePasswordOfAURL(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a database URL": {
			"connect postgres://app:s3cr3t@db.internal:5432/analytics failed",
			"connect postgres://app:[redacted:secret]@db.internal:5432/analytics failed",
		},
		"a URL with no user": {
			"redis://:s3cr3t@cache:6379/0",
			"redis://:[redacted:secret]@cache:6379/0",
		},
		"a password with an at sign in it": {
			"https://ops:p@ss@host.example.com/x",
			"https://ops:[redacted:secret]@host.example.com/x",
		},
		"a user that is an email address": {
			"postgres://me@acme.com:s3cr3t@db/x",
			"postgres://[redacted:email]:[redacted:secret]@db/x",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

func TestText_KeepsAURLWithNoPassword(t *testing.T) {
	for name, in := range map[string]string{ //nolint:gosec // G101: URLs and names that look like credentials but carry none.
		"a port, no user":         "see https://host.example.com:8080/path",
		"a user and no password":  "postgres://app@localhost/analytics",
		"an at sign after a path": "http://localhost:8080/docs/@latest",
		"a host and a port only":  "postgres://localhost:5432",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// Rule 3: known token shapes go whole.
func TestText_RedactsKnownTokenShapes(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"an AWS access key id": {
			"aws id " + awsKeyID + " in the log",
			"aws id [redacted:secret] in the log",
		},
		"a GitHub token": {
			"clone with " + githubPAT + ".",
			"clone with [redacted:secret].",
		},
		"another GitHub token kind": {
			"gho_0123456789abcdefghijklmnopqrstuvwxyzAB",
			"[redacted:secret]",
		},
		"a JWT": {
			"session " + jwt + " expired",
			"session [redacted:secret] expired",
		},
		"a bearer token": {
			"Authorization: Bearer abc123.def456-ghi789",
			"Authorization: [redacted:secret]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

func TestText_KeepsLookalikesOfTokenShapes(t *testing.T) {
	for name, in := range map[string]string{ //nolint:gosec // G101: texts that look like credentials but are not.
		"a 20 character id that is not an AKIA id":       "ASIAIOSFODNN7EXAMPLE",
		"an AKIA id one character short":                 "AKIAIOSFODNN7EXAMPL",
		"a GitHub prefix with a short body":              "ghp_tooshort",
		"a prefix GitHub does not issue":                 "ghx_0123456789abcdefghijklmnopqrstuvwxyzAB",
		"a JWT header with no payload":                   "eyJhbGciOiJIUzI1NiJ9",
		"the word bearer in a sentence":                  "Bearer of bad news",
		"a bearer with a token shorter than a real one":  "Bearer x",
		"an Authorization header with another scheme":    "Authorization: Basic",
		"a base64 text that does not start like a token": "dGhpcyBpcyBub3QgYSB0b2tlbg==",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// Rule 4: a key that names a secret keeps its name, only the value goes.
func TestText_RedactsTheValueOfASecretKey(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"an assignment":       {"password=hunter2", "password=[redacted:secret]"},
		"an upper case key":   {"DB_PASSWORD=hunter2", "DB_PASSWORD=[redacted:secret]"},
		"a mixed case key":    {"Passwd = hunter2", "Passwd = [redacted:secret]"},
		"a YAML entry":        {"token: abc123xyz", "token: [redacted:secret]"},
		"a quoted YAML value": {`secret: "abc 123"`, `secret: "[redacted:secret]"`},
		"a JSON member":       {`{"api_key": "abc123", "n": 1}`, `{"api_key": "[redacted:secret]", "n": 1}`},
		"a JSON member a log escaped": {
			`{"msg":"{\"password\":\"hunter2\",\"user\":\"bob\"}"}`,
			`{"msg":"{\"password\":\"[redacted:secret]\",\"user\":\"bob\"}"}`,
		},
		"a single quoted value": {"apikey = 'abc123'", "apikey = '[redacted:secret]'"},
		"an access key":         {"aws_access_key=abc/def+ghi", "aws_access_key=[redacted:secret]"},
		"a private key":         {"private_key: abc123", "private_key: [redacted:secret]"},
		"a secret key":          {"SECRET_KEY=django-insecure-abc", "SECRET_KEY=[redacted:secret]"},
		"a hyphenated key":      {"api-key: abc123", "api-key: [redacted:secret]"},
		"a hash rocket":         {"token => 'abc123'", "token => '[redacted:secret]'"},
		"a query parameter":     {"GET /x?token=abc123&page=2", "GET /x?token=[redacted:secret]&page=2"},
		"a list of settings":    {"user=bob, password=hunter2, host=db", "user=bob, password=[redacted:secret], host=db"},
		"a value cut by the end of the text": {
			`password="hunter2`,
			`password=[redacted:secret]`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

func TestText_KeepsWhatIsNotASecretValue(t *testing.T) {
	for name, in := range map[string]string{
		"a key that only starts like a secret":    "password_hint",
		"a key that starts like a secret, valued": "password_hint: ask me",
		"a plural":                                 "tokens: 5",
		"a longer word":                            "max_tokens: 100",
		"a secret name in a sentence":              "the password is rotated daily",
		"a column in a select":                     "select password from users",
		"a column definition":                      "password varchar(20) not null",
		"a cast":                                   "select token::text from t",
		"a key with no value":                      "password:",
		"a key whose value is on another line":     "password:\nuser: bob",
		"an empty quoted value":                    `password: ""`,
		"an empty single quoted value":             "password = ''",
		"a marker already standing in the value":   "password=[redacted:secret]",
		"a numbered marker already in the value":   `{"token": "[redacted:secret:3]"}`,
		"a marker already standing behind a colon": "token: [redacted:secret]",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// Rule 5: an email address goes whole.
func TestText_RedactsAnEmailAddress(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"an address":      {"owner is ops@acme.com today", "owner is [redacted:email] today"},
		"a plus address":  {"first.last+tag@sub.example.co.uk,", "[redacted:email],"},
		"two addresses":   {"a@x.org b@y.org", "[redacted:email] [redacted:email]"},
		"in a YAML owner": {"owner: ops@acme.com", "owner: [redacted:email]"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

func TestText_KeepsWhatIsNotAnEmailAddress(t *testing.T) {
	for name, in := range map[string]string{
		"a host with no top level domain": "connect user@localhost",
		"a decorator":                     "@property\ndef x(self): ...",
		"a digit top level domain":        "a@b.1",
		"an at sign with no name":         "send to @acme.com",
		"a version pin":                   "pkg@1.2.3",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// Where two rules want the same text, the earlier rule decides, and the text
// of the loser that lies outside the winner stays.
func TestText_TheEarlierRuleDecidesAnOverlap(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a secret key valued with an email address (rule 4 over rule 5)": {
			"password=ops@acme.com",
			"password=[redacted:secret]",
		},
		"the same, quoted": {
			`"password": "ops@acme.com"`,
			`"password": "[redacted:secret]"`,
		},
		"a secret key and a later email address stay two matches": {
			"password=ops@acme.com, notify bob@acme.com",
			"password=[redacted:secret], notify [redacted:email]",
		},
		"a URL password that is an email address (rule 2 over rule 5)": {
			"https://ops:bob@acme.com@host.example.com/x",
			"https://ops:[redacted:secret]@host.example.com/x",
		},
		"a JWT under a secret key (rule 3 over rule 4)": {
			"token: " + jwt,
			"token: [redacted:secret]",
		},
		"a bearer token under a secret key (rule 3 over rule 4)": {
			"token: Bearer abc123.def456-ghi789",
			"token: [redacted:secret]",
		},
		"a URL under a secret key (rule 2 over rule 4)": {
			"password=postgres://app:s3cr3t@db/x",
			"password=postgres://app:[redacted:secret]@db/x",
		},
		"an AWS key id as the user of an email-looking host (rule 3 over rule 5)": {
			awsKeyID + "@bucket.example.com",
			"[redacted:secret]@bucket.example.com",
		},
		"a private key under a secret key (rule 1 over rule 4)": {
			`{"private_key": "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n", "id": 1}`,
			`{"private_key": "[redacted:key]\n", "id": 1}`,
		},
		"an email inside a private key block (rule 1 over rule 5)": {
			"-----BEGIN PRIVATE KEY-----\nops@acme.com\n-----END PRIVATE KEY-----",
			"[redacted:key]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

// Text is a function of its input only.
func TestText_IsDeterministic(t *testing.T) {
	in := strings.Join([]string{
		pemKey,
		"postgres://app:s3cr3t@db/x",
		awsKeyID,
		"password=ops@acme.com",
		"mail bob@acme.com",
		"token: " + jwt,
	}, "\n")
	want := strings.Join([]string{
		"[redacted:key]",
		"postgres://app:[redacted:secret]@db/x",
		"[redacted:secret]",
		"password=[redacted:secret]",
		"mail [redacted:email]",
		"token: [redacted:secret]",
	}, "\n")

	for range 20 {
		assert.Equal(t, want, sanitize.Text(in))
	}
}

// A marker never matches a rule, so a cleaned text is its own clean.
func TestText_CleaningTwiceChangesNothingMore(t *testing.T) {
	for name, in := range map[string]string{
		"every rule": strings.Join([]string{
			pemKey, "postgres://app:s3cr3t@db/x", awsKeyID, githubPAT, jwt,
			"Authorization: Bearer abc123.def456-ghi789", "password=hunter2", "mail bob@acme.com",
		}, "\n"),
		"overlaps": strings.Join([]string{
			"password=ops@acme.com",
			"password=postgres://app:s3cr3t@db/x",
			"token: " + jwt,
			"https://ops:p@ss@host.example.com/x",
			`{"private_key": "` + strings.ReplaceAll(pemKey, "\n", `\n`) + `"}`,
		}, "\n"),
		"nothing to redact": "select 1 from t where a = 'b'",
	} {
		t.Run(name, func(t *testing.T) {
			once := sanitize.Text(in)
			assert.Equal(t, once, sanitize.Text(once))
		})
	}
}

// A secret never survives a rule that names it, wherever the rule's text sits.
func TestText_LeavesNoSecretBehind(t *testing.T) {
	secrets := []string{"s3cr3t", "hunter2", awsKeyID, githubPAT, jwt, "MIIEowIBAAKCAQEAxJ5g1c0Zk3lQ", "ops@acme.com", "p@ss"}
	in := strings.Join([]string{
		"dsn=postgres://app:s3cr3t@db/x password=hunter2",
		awsKeyID + " " + githubPAT + " " + jwt,
		pemKey,
		"contact ops@acme.com; https://u:p@ss@h.example.com",
	}, "\n")

	out := sanitize.Text(in)
	for _, secret := range secrets {
		assert.NotContains(t, out, secret)
	}
}

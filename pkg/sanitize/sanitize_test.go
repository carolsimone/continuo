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
			`password="[redacted:secret]`,
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
		"an empty triple quoted value":             `password = """"""`,
		"an empty value and a quoted one after it": `password = "" or "x"`,
		"a marker already standing in the value":   "password=[redacted:secret]",
		"a numbered marker already in the value":   `{"token": "[redacted:secret:3]"}`,
		"a marker already standing behind a colon": "token: [redacted:secret]",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// A hardcoded credential in an annotated assignment sits after the annotation:
// the quoted literal goes and the annotation stays.
func TestText_RedactsTheLiteralOfAnAnnotatedAssignment(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a plain annotation": {
			`password: str = "hunter2"`,
			`password: str = "[redacted:secret]"`,
		},
		"a subscripted annotation": {
			`api_key: Optional[str] = "sk-live-abc123"`,
			`api_key: Optional[str] = "[redacted:secret]"`,
		},
		"a Final constant": {
			`PASSWORD: Final = "hunter2"`,
			`PASSWORD: Final = "[redacted:secret]"`,
		},
		"a single quoted literal": {
			`token: str = 'abc123'`,
			`token: str = '[redacted:secret]'`,
		},
		"a union annotation on an attribute": {
			`self.secret: str | None = "abc123"`,
			`self.secret: str | None = "[redacted:secret]"`,
		},
		"a nested annotation": {
			`token: Dict[str, List[int]] = "abc123"`,
			`token: Dict[str, List[int]] = "[redacted:secret]"`,
		},
		"a typed TypeScript constant": {
			`const password: string = "hunter2";`,
			`const password: string = "[redacted:secret]";`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

// A quoted value is redacted as the whole literal: a quote written doubled or
// after a backslash stays inside it, a triple-quoted literal runs over lines
// to its closing quotes, a string prefix stays outside it, and a literal cut by
// the end of the text runs to that end.
func TestText_RedactsAWholeQuotedLiteral(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a SQL literal with a doubled quote": {
			"password = 'first''second'", "password = '[redacted:secret]'",
		},
		"a doubled double quote": {
			`password = "first""second"`, `password = "[redacted:secret]"`,
		},
		"a literal that starts with a doubled quote": {
			"password = '''quoted'' tail'", "password = '''[redacted:secret]",
		},
		"a backslash escaped quote": {
			`token = 'it\'s-x9'`, `token = '[redacted:secret]'`,
		},
		"a Python triple double quoted literal": {
			`password = """hunter2"""`, `password = """[redacted:secret]"""`,
		},
		"a Python triple single quoted literal": {
			"password = '''hunter2'''", "password = '''[redacted:secret]'''",
		},
		"a triple quoted literal over lines": {
			"password = \"\"\"line-one\nline-two\"\"\"\nuser = 'bob'",
			"password = \"\"\"[redacted:secret]\"\"\"\nuser = 'bob'",
		},
		"a triple quoted literal that holds quotes": {
			`token = """ab"c""d"""`, `token = """[redacted:secret]"""`,
		},
		"a triple quoted literal that ends in a quote": {
			`token = """abc""""`, `token = """[redacted:secret]"""`,
		},
		"a triple quoted literal cut by the end of the text": {
			"password = \"\"\"hunter2\nmore", "password = \"\"\"[redacted:secret]",
		},
		"a doubled quote in a literal cut by the end of the text": {
			"password='first''second", "password='[redacted:secret]",
		},
		"a bytes literal": {
			`password = b"hunter2"`, `password = b"[redacted:secret]"`,
		},
		"a raw triple quoted literal": {
			`token = r"""a\b"""`, `token = r"""[redacted:secret]"""`,
		},
		"an annotated triple quoted literal": {
			`password: str = """hunter2"""`, `password: str = """[redacted:secret]"""`,
		},
		"an annotated literal with a doubled quote": {
			"token: str = 'first''second'", "token: str = '[redacted:secret]'",
		},
		"a log escaped literal holding an escaped quote": {
			`{"msg":"password=\"p\\\"wsecretpart\""}`, `{"msg":"password=\"[redacted:secret]\""}`,
		},
		"a log escaped triple quoted literal": {
			`{"msg":"password = \"\"\"tri\"\"\" x"}`, `{"msg":"password = \"\"\"[redacted:secret]\"\"\" x"}`,
		},
		"a log escaped triple quoted literal holding a quote": {
			`{"msg":"token = \"\"\"a\"b\"\"\""}`, `{"msg":"token = \"\"\"[redacted:secret]\"\"\""}`,
		},
		"a log escaped triple quoted literal cut by the end of the text": {
			`password = \"\"\"tri`, `password = \"\"\"[redacted:secret]`,
		},
		"an empty literal and then a value": {
			`{"password": "", "token": "abc123"}`, `{"password": "", "token": "[redacted:secret]"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
			assert.Equal(t, tc.want, sanitize.Text(tc.want), "cleaned again")
		})
	}
}

// An unquoted value is a literal only. Code that reads a secret from somewhere
// else holds none, and cutting it mid-token protects nothing.
func TestText_KeepsAnUnquotedValueThatIsNotALiteral(t *testing.T) {
	for name, in := range map[string]string{
		"a subscript of an environment":            `token = os.environ["TOKEN"]`,
		"a call":                                   `api_key=os.getenv("API_KEY")`,
		"a call in a template tag":                 `{% set token = var('x') %}`,
		"a method call":                            `self.token = self.token_from(x)`,
		"an attribute":                             `a.token = b.token`,
		"a dotted name":                            `password = settings.DB_PASSWORD`,
		"an attribute in a SQL condition":          `where a.token = b.token`,
		"an attribute of self":                     `token = self.token`,
		"a comparison of attributes":               `if x.token == y.token:`,
		"a Go short declaration":                   `token := cfg.Token`,
		"a hash rocket to an attribute":            `token => opts.token`,
		"an attribute after a tab":                 "password\t=\tsettings.PASSWORD",
		"a dotted type in a parameter list":        `def f(token: typing.Any):`,
		"a dotted secret type and another param":   `def f(password: pydantic.SecretStr, b: int):`,
		"a dotted optional type":                   `def f(token: t.Optional):`,
		"a type in a parameter list":               `def connect(token: str, host: str):`,
		"a type that closes the parameters":        `def connect(host: str, password: SecretStr):`,
		"a field with a type only":                 "password: str\n",
		"a field with a typed default":             `password: Optional[str] = None`,
		"a custom type with a call as its default": `token: Custom = os.getenv("T")`,
		"a template expression":                    `password: {{ env_var('DBT_PASSWORD') }}`,
		"a shell expansion":                        `token: ${TOKEN}`,
		"a command substitution":                   `password=$(cat /run/secrets/pw)`,
		"a JSON null":                              `{"password": null, "user": "bob"}`,
		"a Python None":                            `password = None`,
		"a boolean":                                `token: false`,
		"a YAML literal block":                     "password: |\n  line\n",
		"a YAML folded block":                      "token: >-\n  line\n",
		"a YAML literal block with a chomp":        "token: |-\n  line\n",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// A password may hold a bracket. The value is a reference only when the bracket
// opens a call or a subscript: a quote follows it, or it closes on the same
// line at the end of a word, and the name before it holds no digit.
func TestText_RedactsAPasswordThatHoldsABracket(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"an open parenthesis":                {"password: xK9(mP2q", "password: [redacted:secret]"},
		"a parenthesis closed before more":   {"password=xK9(mP2)q", "password=[redacted:secret])q"},
		"a bracket closed before more":       {"PGPASSWORD=Ab[cd]9 psql", "PGPASSWORD=[redacted:secret]]9 psql"},
		"an open bracket after a digit name": {"password: Tr0ub4dor[3", "password: [redacted:secret]"},
		"a closed call whose name has a digit": {
			"password=xK9(mP2) psql", "password=[redacted:secret]) psql",
		},
		"an open parenthesis and a quote after the word": {"password: ab(cd'ef", "password: [redacted:secret]'ef"},
		"a bracket that never closes on the line":        {"token=Abc(def\nnext)", "token=[redacted:secret]\nnext)"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

// Code that reads a value keeps its call or subscript: a quote follows the
// bracket, or the bracket closes on the same line at the end of the word.
func TestText_KeepsACallOrSubscriptThatReadsAValue(t *testing.T) {
	for name, in := range map[string]string{
		"a subscript with a quoted key":   `token = os.environ["TOKEN"]`,
		"a call with a quoted argument":   `api_key=os.getenv("API_KEY")`,
		"a template call":                 `{% set token = var('x') %}`,
		"a call with a name argument":     `self.token = self.token_from(x)`,
		"a call with no argument":         `token = get_secret()`,
		"a call with two arguments":       `token = os.getenv(KEY, "default")`,
		"a subscript with a name":         `token = config[key]`,
		"a chained call":                  `password = os.environ.get("P").strip()`,
		"a call followed by an attribute": `token = vault.read(path).value`,
		"a call in a list":                `[token=lookup(a), x]`,
		"a call cut after its quote":      `token = os.getenv("API_KEY`,
		"a call with a bracket inside":    `secret = fetch(names[0])`,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, in, sanitize.Text(in))
		})
	}
}

// A bare word that the syntax does not mark as a reference is still a value.
func TestText_RedactsABareWordThatIsNotAReference(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a password":                   {"password=hunter2", "password=[redacted:secret]"},
		"a Postgres variable in a log": {"PGPASSWORD=s3cr3t psql -h db", "PGPASSWORD=[redacted:secret] psql -h db"},
		"a value ending a sentence":    {"the password=hunter2.", "the password=[redacted:secret]"},
		"a dotted value with digits":   {"client_secret=abc123.def456", "client_secret=[redacted:secret]"},
		"a three part dotted value":    {"token: a1b2c3d4e5f6.g7h8i9.j0k1l2m3n4", "token: [redacted:secret]"},
		"a YAML value":                 {"password: hunter2", "password: [redacted:secret]"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

// A dotted value is a literal wherever configuration writes it: after a colon
// (YAML, a JSON or JS object) or after an equals sign with no space around it
// (a .env file, a shell, a connection string, a keyword argument). Only code
// written with a space on each side of its operator reads it as a reference.
func TestText_RedactsADottedValueThatConfigurationWrites(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a YAML password":                             {"password: correct.horse", "password: [redacted:secret]"},
		"a .env password":                             {"PASSWORD=correct.horse", "PASSWORD=[redacted:secret]"},
		"a three part YAML secret":                    {"secret: a.b.c", "secret: [redacted:secret]"},
		"a shell export":                              {"export DB_PASSWORD=correct.horse", "export DB_PASSWORD=[redacted:secret]"},
		"a connection string":                         {"host=db password=correct.horse user=bob", "host=db password=[redacted:secret] user=bob"},
		"a keyword argument":                          {"connect(password=correct.horse)", "connect(password=[redacted:secret])"},
		"a keyword argument read from settings":       {"connect(api_key=settings.X)", "connect(api_key=[redacted:secret])"},
		"a dotted name after a colon that is no type": {"password: typing.Horse", "password: [redacted:secret]"},
		"a JS object member":                          {"{ token: this.token }", "{ token: [redacted:secret] }"},
		"a space before the operator":                 {"password =correct.horse", "password =[redacted:secret]"},
		"a space after the operator":                  {"password= correct.horse", "password= [redacted:secret]"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
		})
	}
}

// The userinfo of a URL ends before the first slash, question mark or number
// sign, so a later at sign of the query is not a password's.
func TestText_AURLsUserinfoEndsBeforeAQueryOrFragment(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a port and an email in the query": {
			"https://example.com:443?email=a@b.com",
			"https://example.com:443?email=[redacted:email]",
		},
		"an email in a nested URL": {
			"https://example.com?next=mailto:bob@acme.com",
			"https://example.com?next=mailto:[redacted:email]",
		},
		"an email in the fragment": {
			"https://example.com:443#ops@acme.com",
			"https://example.com:443#[redacted:email]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
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

// Where two rules want the same text, the earlier match stands and the later
// one is dropped, except that the value of a secret key takes in the earlier
// matches inside it, so a value is redacted whole.
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
		"a URL under a secret key (rule 4 takes in rule 2)": {
			"password=postgres://app:s3cr3t@db/x",
			"password=[redacted:secret]",
		},
		"an AWS key id as the user of an email-looking host (rule 3 over rule 5)": {
			awsKeyID + "@bucket.example.com",
			"[redacted:secret]@bucket.example.com",
		},
		"a private key under a secret key (rule 4 takes in rule 1)": {
			`{"private_key": "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n", "id": 1}`,
			`{"private_key": "[redacted:key]", "id": 1}`,
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
		"values a known token cut short": strings.Join([]string{
			"token:Bearer 00000000!",
			"password=AKIAIOSFODNN7EXAMPLEfoo",
			"token=x1AKIAIOSFODNN7EXAMPLE",
			"secret: " + githubPAT + "-tail",
		}, "\n"),
		"nothing to redact": "select 1 from t where a = 'b'",
	} {
		t.Run(name, func(t *testing.T) {
			once := sanitize.Text(in)
			assert.Equal(t, once, sanitize.Text(once))
		})
	}
}

// A value is redacted whole: a known token, a marker or a URL password inside
// it, with text glued to it, takes the rest of the value with it, on the first
// cleaning and on any text that was cleaned already.
func TestText_ARedactedPartOfAValueTakesTheWholeValueWithIt(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"a bearer token and a tail":         {"token:Bearer 00000000!", "token:[redacted:secret]"},
		"a key id and a tail":               {"password=AKIAIOSFODNN7EXAMPLEfoo", "password=[redacted:secret]"},
		"a key id and a head":               {"password=x1AKIAIOSFODNN7EXAMPLE", "password=[redacted:secret]"},
		"a GitHub token and a tail":         {"token=" + githubPAT + ".tail", "token=[redacted:secret]"},
		"a GitHub token and a dashed tail":  {"secret: " + githubPAT + "-tail", "secret: [redacted:secret]"},
		"a marker and a tail":               {"password=[redacted:secret]hunter2", "password=[redacted:secret]"},
		"a numbered marker and a tail":      {"password=[redacted:secret:1]hunter2", "password=[redacted:secret]"},
		"a marker in the middle of a value": {"token=ab[redacted:secret]cd", "token=[redacted:secret]"},
		"a marker in the middle of a URL password": {
			"postgres://app:s3[redacted:secret]cr@db/x",
			"postgres://app:[redacted:secret]@db/x",
		},
		"a marker as the user of a URL password": {
			"https://[redacted:secret]:pw@h.example.com/x",
			"https://[redacted:secret]:[redacted:secret]@h.example.com/x",
		},
		"a value that is a marker": {"password=[redacted:secret]", "password=[redacted:secret]"},
		"a value with a marker, then an equals sign": {
			"password: [redacted:secret]x = 1", "password: [redacted:secret] = 1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitize.Text(tc.in))
			assert.Equal(t, tc.want, sanitize.Text(tc.want), "cleaned again")
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

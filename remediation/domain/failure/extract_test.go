package failure

import "testing"

func TestExtractDbtFilePath(t *testing.T) {
	cases := []struct {
		name    string
		logText string
		want    string
	}{
		{
			name:    "compilation error with path in parens",
			logText: "Compilation Error in model daily_transactions (models/daily_transactions.sql)\n  got '='",
			want:    "models/daily_transactions.sql",
		},
		{
			name:    "yaml file in models",
			logText: "Error parsing file models/staging/schema.yml: unexpected key",
			want:    "models/staging/schema.yml",
		},
		{
			name:    "seed csv file",
			logText: "Compilation Error in seeds/ref_data.csv line 5",
			want:    "seeds/ref_data.csv",
		},
		{
			name:    "no path in log",
			logText: "Database Error: connection refused",
			want:    "",
		},
		{
			name:    "empty log",
			logText: "",
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractDbtFilePath(tc.logText)
			if got != tc.want {
				t.Errorf("extractDbtFilePath(%q) = %q, want %q", tc.logText, got, tc.want)
			}
		})
	}
}

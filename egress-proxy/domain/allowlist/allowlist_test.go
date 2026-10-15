package allowlist

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNew(t *testing.T, entries ...Entry) *Allowlist {
	t.Helper()
	a, err := New(entries)
	require.NoError(t, err)
	return a
}

func TestMatch_ExactHostIgnoresCaseAndTrailingDot(t *testing.T) {
	a := mustNew(t, Entry{Host: "API.Anthropic.com", Port: 443})
	for _, host := range []string{"api.anthropic.com", "API.ANTHROPIC.COM", "api.anthropic.com."} {
		e, ok := a.Match(host, 443)
		require.True(t, ok, host)
		assert.Equal(t, "api.anthropic.com", e.Host)
	}
	_, ok := a.Match("api.anthropic.com", 80)
	assert.False(t, ok, "another port")
	_, ok = a.Match("anthropic.com", 443)
	assert.False(t, ok, "the parent domain")
	_, ok = a.Match("evil-api.anthropic.com", 443)
	assert.False(t, ok, "a sibling name")
}

func TestMatch_WildcardCoversNamesBelowNotTheApex(t *testing.T) {
	a := mustNew(t, Entry{Host: "*.example.com", Port: 443})
	_, ok := a.Match("api.example.com", 443)
	assert.True(t, ok)
	_, ok = a.Match("a.b.example.com", 443)
	assert.True(t, ok)
	_, ok = a.Match("example.com", 443)
	assert.False(t, ok, "the apex")
	_, ok = a.Match("badexample.com", 443)
	assert.False(t, ok, "a name that only ends with the same letters")
	_, ok = a.Match("api.example.com", 8443)
	assert.False(t, ok, "another port")
	_, ok = a.Match("a..example.com", 443)
	assert.False(t, ok, "an empty label")
}

func TestMatch_ExactEntryWinsThenLongestWildcard(t *testing.T) {
	a := mustNew(t,
		Entry{Host: "*.example.com", Port: 443},
		Entry{Host: "*.internal.example.com", Port: 443, AllowPrivate: true},
		Entry{Host: "vault.internal.example.com", Port: 443},
	)
	e, _ := a.Match("db.internal.example.com", 443)
	assert.True(t, e.AllowPrivate, "the longest wildcard applies")
	e, _ = a.Match("vault.internal.example.com", 443)
	assert.False(t, e.AllowPrivate, "an exact entry beats every wildcard")
	e, _ = a.Match("api.example.com", 443)
	assert.False(t, e.AllowPrivate)
}

func TestNew_MergesDuplicates(t *testing.T) {
	a := mustNew(t, Entry{Host: "minio.storage.svc", Port: 9000}, Entry{Host: "MINIO.storage.svc", Port: 9000, AllowPrivate: true})
	assert.Equal(t, 1, a.Len())
	e, ok := a.Match("minio.storage.svc", 9000)
	require.True(t, ok)
	assert.True(t, e.AllowPrivate)
}

func TestNew_RejectsInvalidEntries(t *testing.T) {
	label64 := strings.Repeat("a", 64)
	kelvin := string(rune(0x212A))
	tooLong := strings.Repeat(strings.Repeat("a", 63)+".", 4) + "com"
	for _, e := range []Entry{
		{Host: "", Port: 443},
		{Host: "127.1", Port: 443},
		{Host: "2130706433", Port: 443},
		{Host: "0x7f.0.0.1", Port: 443},
		{Host: "doc" + kelvin + "er.io", Port: 443},
		{Host: tooLong, Port: 443},
		{Host: "1.2.3.4", Port: 443},
		{Host: "[::1]", Port: 443},
		{Host: "::1", Port: 443},
		{Host: "*", Port: 443},
		{Host: "*.com", Port: 443},
		{Host: "a.*.example.com", Port: 443},
		{Host: "*foo.example.com", Port: 443},
		{Host: "-a.example.com", Port: 443},
		{Host: "a-.example.com", Port: 443},
		{Host: "a_b.example.com", Port: 443},
		{Host: label64 + ".example.com", Port: 443},
		{Host: "api.example.com", Port: 0},
	} {
		_, err := New([]Entry{e})
		require.Error(t, err, "%+v", e)
		assert.True(t, errors.Is(err, ErrInvalidEntry), "%+v", e)
	}
}

func TestNew_AcceptsSingleLabelHosts(t *testing.T) {
	a := mustNew(t, Entry{Host: "stub-llm", Port: 9100, AllowPrivate: true})
	_, ok := a.Match("stub-llm", 9100)
	assert.True(t, ok)
}

func TestNew_AcceptsValidNamesNearTheRejectedForms(t *testing.T) {
	// 253 bytes: three 63-byte labels, then a 61-byte label.
	longest := strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 63) + "." + strings.Repeat("a", 61)
	require.Len(t, longest, 253)
	a := mustNew(t,
		Entry{Host: "stub-llm", Port: 9100},
		Entry{Host: "svc1.example.com", Port: 443},
		Entry{Host: "a1", Port: 443},
		Entry{Host: longest, Port: 443},
	)
	assert.Equal(t, 4, a.Len())
	_, ok := a.Match("svc1.example.com", 443)
	assert.True(t, ok)
	_, ok = a.Match("a1", 443)
	assert.True(t, ok)
}

func TestMatch_NonASCIIHostMatchesNothing(t *testing.T) {
	kelvin := string(rune(0x212A))
	a := mustNew(t, Entry{Host: "docker.io", Port: 443})
	_, ok := a.Match("doc"+kelvin+"er.io", 443)
	assert.False(t, ok, "the Kelvin sign lower-cases to k under Unicode folding")
	_, ok = a.Match("docKer.io", 443)
	assert.True(t, ok, "ASCII case is still ignored")
}

func TestMatch_ReturnsTheNormalizedEntryHost(t *testing.T) {
	a := mustNew(t, Entry{Host: "Docker.IO.", Port: 443})
	e, ok := a.Match("DOCKER.io", 443)
	require.True(t, ok)
	assert.Equal(t, Normalize("DOCKER.io"), e.Host)
}

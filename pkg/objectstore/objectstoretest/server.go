package objectstoretest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
)

// Request is one request a Server received.
type Request struct {
	Method        string
	Path          string
	Query         url.Values
	Authorization string
	ContentType   string
	Body          []byte
}

// Server stands in for S3 over HTTP: it records every request, then answers
// it with the handler it was built with.
type Server struct {
	*httptest.Server
	mu       sync.Mutex
	requests []Request
}

// NewServer starts a Server; the test's cleanup closes it. The handler can
// still read the request body after it was recorded.
func NewServer(t testing.TB, handler http.HandlerFunc) *Server {
	t.Helper()
	s := &Server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, Request{
			Method:        r.Method,
			Path:          r.URL.Path,
			Query:         r.URL.Query(),
			Authorization: r.Header.Get("Authorization"),
			ContentType:   r.Header.Get("Content-Type"),
			Body:          body,
		})
		s.mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

// Requests returns the requests received so far, in arrival order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// NoSuchKey answers as S3 does for a key that holds no object.
func NoSuchKey(w http.ResponseWriter, r *http.Request) {
	ErrorStatus(http.StatusNotFound, "NoSuchKey")(w, r)
}

// ErrorStatus answers with an S3 error document carrying code, under status.
func ErrorStatus(status int, code string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + code + `</Code><Message>` + code + `</Message></Error>`))
	}
}

// Object answers with body as the object's content, declaring its length.
func Object(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}
}

// DeclaredLength answers 200 with a Content-Length of n and no body: it stands
// in for an object of n bytes, which a reader that checks the declared size
// refuses before reading.
func DeclaredLength(n int64) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
	}
}

// OK answers 200 with no body, as S3 does for a stored object.
func OK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

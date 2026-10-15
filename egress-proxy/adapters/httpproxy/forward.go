package httpproxy

import (
	"net/http"
	"net/http/httputil"
)

func newForwarder(Authorizer, func(http.ResponseWriter, *http.Request, error)) *httputil.ReverseProxy {
	return nil
}

func (p *Proxy) forward(w http.ResponseWriter, r *http.Request) {
	port, _ := urlPort(r.URL)
	p.refuse(w, r, r.URL.Hostname(), port, http.StatusMethodNotAllowed, ReasonUnsupported, "plain HTTP is not forwarded")
}

func (p *Proxy) forwardFailed(http.ResponseWriter, *http.Request, error) {}

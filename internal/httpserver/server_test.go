package httpserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"vilog-victorialogs/internal/config"
)

type fakeChecker struct {
	name string
	err  error
}

func (f fakeChecker) Name() string {
	return f.name
}

func (f fakeChecker) Ping(context.Context) error {
	return f.err
}

func TestHealthzReturnsOK(t *testing.T) {
	server := newTestServer(t, nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	server.engine.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestIndexDisablesCacheAndVersionsAssets(t *testing.T) {
	server := newTestServer(t, nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)

	server.engine.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if cacheControl := recorder.Header().Get("Cache-Control"); !strings.Contains(cacheControl, "no-store") {
		t.Fatalf("cache-control = %q, want no-store", cacheControl)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "/assets/app.js?v=") {
		t.Fatalf("index body does not contain versioned app.js asset")
	}
	if !strings.Contains(body, "/assets/styles.css?v=") {
		t.Fatalf("index body does not contain versioned styles.css asset")
	}
}

func TestReadyzReturns503WhenDependencyFails(t *testing.T) {
	server := newTestServer(t, []ReadinessChecker{
		fakeChecker{name: "mongo", err: errors.New("unreachable")},
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.engine.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestReadyzReturns200WhenDependenciesPass(t *testing.T) {
	server := newTestServer(t, []ReadinessChecker{
		fakeChecker{name: "mongo"},
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	server.engine.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func newTestServer(t *testing.T, checkers []ReadinessChecker) *Server {
	t.Helper()

	cfg := config.Default()
	cfg.Logging.Development = false

	server, err := New(cfg, zap.NewNop(), checkers, BuildInfo{Version: "test"}, Dependencies{})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	return server
}

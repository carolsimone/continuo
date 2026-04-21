//go:build integration

package cli_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type stubStateServer struct {
	statev1.UnimplementedStateServiceServer
	resp *statev1.TriggerScheduleResponse
}

func (s *stubStateServer) TriggerSchedule(_ context.Context, _ *statev1.TriggerScheduleRequest) (*statev1.TriggerScheduleResponse, error) {
	return s.resp, nil
}

func TestCLI_TriggerScheduleAgainstInProcessServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := lis.Addr().String()

	grpcSrv := grpc.NewServer()
	statev1.RegisterStateServiceServer(grpcSrv, &stubStateServer{
		resp: &statev1.TriggerScheduleResponse{ScheduleId: "sched_integ_1"},
	})
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.GracefulStop()

	binPath := filepath.Join(t.TempDir(), "continuo")
	buildCmd := exec.Command("go", "build", "-o", binPath, "./cmd/continuo")
	buildCmd.Stderr = os.Stderr
	require.NoError(t, buildCmd.Run())

	out, err := exec.Command(binPath, "--endpoint", endpoint, "schedule", "trigger", "daily_ingest").Output()
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(out, &payload))
	assert.Equal(t, "sched_integ_1", payload["schedule_id"])
	assert.Equal(t, "daily_ingest", payload["schedule_name"])
	assert.NotEmpty(t, payload["triggered_at"])
}

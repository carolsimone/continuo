package grpc_test

import (
	"testing"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// The promotion-seq fields keep the field numbers every copy of
// orchestrator.proto decodes them by (ui and cli vendor their own copies), so a
// client and a server built from different copies read the same value during a
// rolling update. The expected bytes are the raw wire encoding: tag
// (field<<3 | wire type) followed by the varint value.
func TestPromotionSeqFields_KeepTheirWireNumbers(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
		want []byte
	}{
		{
			name: "GetScheduleGraphResponse.promotion_seq is field 3",
			msg:  &orchestratorv1.GetScheduleGraphResponse{PromotionSeq: 42},
			want: []byte{0x18, 42},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := proto.MarshalOptions{Deterministic: true}.Marshal(tc.msg)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

package client

import (
	"context"

	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// DeadLetterClient is the narrow interface the dlq commands depend on.
type DeadLetterClient interface {
	List(ctx context.Context, source, stream string) (*deadletterv1.ListDeadLettersResponse, error)
	Get(ctx context.Context, id string) (*deadletterv1.GetDeadLetterResponse, error)
	Redrive(ctx context.Context, ids []string, reason, actor string) (*deadletterv1.RedriveDeadLettersResponse, error)
	Close() error
}

// NewDeadLetterClient dials dead-letter-controller. The returned client must
// be Closed by the caller.
func NewDeadLetterClient(_ context.Context, endpoint string) (DeadLetterClient, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(maintenanceInterceptor))
	if err != nil {
		return nil, err
	}
	return &deadLetterGRPCClient{conn: conn, api: deadletterv1.NewDeadLetterServiceClient(conn)}, nil
}

type deadLetterGRPCClient struct {
	conn *grpc.ClientConn
	api  deadletterv1.DeadLetterServiceClient
}

func (c *deadLetterGRPCClient) List(ctx context.Context, source, stream string) (*deadletterv1.ListDeadLettersResponse, error) {
	return c.api.ListDeadLetters(ctx, &deadletterv1.ListDeadLettersRequest{Source: source, Stream: stream})
}

func (c *deadLetterGRPCClient) Get(ctx context.Context, id string) (*deadletterv1.GetDeadLetterResponse, error) {
	return c.api.GetDeadLetter(ctx, &deadletterv1.GetDeadLetterRequest{Id: id})
}

func (c *deadLetterGRPCClient) Redrive(ctx context.Context, ids []string, reason, actor string) (*deadletterv1.RedriveDeadLettersResponse, error) {
	if actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, userIDMetadataKey, actor)
	}
	return c.api.RedriveDeadLetters(ctx, &deadletterv1.RedriveDeadLettersRequest{Ids: ids, Reason: reason})
}

func (c *deadLetterGRPCClient) Close() error { return c.conn.Close() }

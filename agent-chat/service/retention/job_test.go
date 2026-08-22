package retention

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubRepo struct {
	idle      []domain.Thread
	deleted   []uuid.UUID
	nowActive map[uuid.UUID]bool // threads that became active after selection
}

func (s *stubRepo) ListIdleThreads(context.Context, time.Time, int) ([]domain.Thread, error) {
	out := s.idle
	s.idle = nil // second page is empty
	return out, nil
}
func (s *stubRepo) ListMessages(context.Context, uuid.UUID) ([]domain.Message, error) {
	return []domain.Message{{Role: domain.RoleUser}}, nil
}
func (s *stubRepo) DeleteThreadIfIdle(_ context.Context, id uuid.UUID, _ time.Time) (bool, error) {
	if s.nowActive[id] {
		return false, nil // raced: a message arrived, no longer idle
	}
	s.deleted = append(s.deleted, id)
	return true, nil
}

type recordingArchiver struct{ archived []uuid.UUID }

func (a *recordingArchiver) ArchiveThread(_ context.Context, t domain.Thread, _ []domain.Message) error {
	a.archived = append(a.archived, t.ID)
	return nil
}

func TestSweep_DeletesIdleThreads(t *testing.T) {
	th := domain.Thread{ID: uuid.New()}
	repo := &stubRepo{idle: []domain.Thread{th}}
	job := NewJob(repo, nil, 30, nil)
	require.NoError(t, job.Sweep(context.Background()))
	assert.Equal(t, []uuid.UUID{th.ID}, repo.deleted)
}

func TestSweep_ArchivesBeforeDeleteWhenConfigured(t *testing.T) {
	th := domain.Thread{ID: uuid.New()}
	repo := &stubRepo{idle: []domain.Thread{th}}
	arch := &recordingArchiver{}
	job := NewJob(repo, arch, 30, nil)
	require.NoError(t, job.Sweep(context.Background()))
	assert.Equal(t, []uuid.UUID{th.ID}, arch.archived)
	assert.Equal(t, []uuid.UUID{th.ID}, repo.deleted)
}

func TestSweep_SkipsThreadThatBecameActive(t *testing.T) {
	th := domain.Thread{ID: uuid.New()}
	// Selected as idle, but a message arrived before the conditional delete.
	repo := &stubRepo{idle: []domain.Thread{th}, nowActive: map[uuid.UUID]bool{th.ID: true}}
	job := NewJob(repo, nil, 30, nil)
	require.NoError(t, job.Sweep(context.Background()))
	assert.Empty(t, repo.deleted, "a now-active thread must not be deleted")
}

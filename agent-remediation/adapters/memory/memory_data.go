package memory

import (
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// data is every table of the in-memory database. A unit of work works on a
// copy, made by clone.
type data struct {
	proposals    []*proposalRow
	outbox       []*outbox.Entry
	messages     []*messageprocessing.MessageProcessing
	remediations map[uuid.UUID]*remediation.Remediation
	tries        map[tryKey]*remediation.Try
	clusters     map[clusterKey]*remediation.Cluster
	steps        []*remediation.StepRun
	lastStepID   int64
}

type tryKey struct {
	remediationID uuid.UUID
	try           int
}

type clusterKey struct {
	remediationID uuid.UUID
	try           int
	cluster       int
}

// proposalRow is one row of the proposal table.
type proposalRow struct {
	id          string
	p           proposal.Proposal
	verifyError string
}

func newData() *data {
	return &data{
		remediations: map[uuid.UUID]*remediation.Remediation{},
		tries:        map[tryKey]*remediation.Try{},
		clusters:     map[clusterKey]*remediation.Cluster{},
	}
}

// clone returns a deep copy of d.
func (d *data) clone() *data {
	c := newData()
	c.lastStepID = d.lastStepID
	for _, row := range d.proposals {
		c.proposals = append(c.proposals, &proposalRow{id: row.id, p: cloneProposal(row.p), verifyError: row.verifyError})
	}
	for _, e := range d.outbox {
		entry := cloneOutboxEntry(*e)
		c.outbox = append(c.outbox, &entry)
	}
	for _, m := range d.messages {
		msg := cloneMessage(*m)
		c.messages = append(c.messages, &msg)
	}
	for id, r := range d.remediations {
		rem := cloneRemediation(*r)
		c.remediations[id] = &rem
	}
	for k, t := range d.tries {
		try := cloneTry(*t)
		c.tries[k] = &try
	}
	for k, cl := range d.clusters {
		cluster := cloneCluster(*cl)
		c.clusters[k] = &cluster
	}
	for _, s := range d.steps {
		step := cloneStep(*s)
		c.steps = append(c.steps, &step)
	}
	return c
}

// sortedRemediations returns the remediation rows oldest first, by creation
// time then id, the order the Postgres claim reads them in.
func (d *data) sortedRemediations() []*remediation.Remediation {
	out := make([]*remediation.Remediation, 0, len(d.remediations))
	for _, r := range d.remediations {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func cloneProposal(p proposal.Proposal) proposal.Proposal {
	p.ResolvedNodeIDs = slices.Clone(p.ResolvedNodeIDs)
	p.NodeOutcomes = maps.Clone(p.NodeOutcomes)
	p.Services = slices.Clone(p.Services)
	p.TriggerPayload = slices.Clone(p.TriggerPayload)
	p.Verifications = cloneVerifications(p.Verifications)
	if p.Edits != nil {
		edits := make([]proposal.FileEdit, len(p.Edits))
		for i, e := range p.Edits {
			e.MemberNodeIDs = slices.Clone(e.MemberNodeIDs)
			edits[i] = e
		}
		p.Edits = edits
	}
	return p
}

func cloneVerifications(vs []proposal.Verification) []proposal.Verification {
	if vs == nil {
		return nil
	}
	out := make([]proposal.Verification, len(vs))
	for i, v := range vs {
		v.ActivatedAt = cloneTimePtr(v.ActivatedAt)
		out[i] = v
	}
	return out
}

func cloneOutboxEntry(e outbox.Entry) outbox.Entry {
	e.Payload = slices.Clone(e.Payload)
	if e.MessageProcessingID != nil {
		id := *e.MessageProcessingID
		e.MessageProcessingID = &id
	}
	e.ProcessedAt = cloneTimePtr(e.ProcessedAt)
	e.NextAttemptAt = cloneTimePtr(e.NextAttemptAt)
	if e.ErrorMessage != nil {
		msg := *e.ErrorMessage
		e.ErrorMessage = &msg
	}
	return e
}

func cloneMessage(m messageprocessing.MessageProcessing) messageprocessing.MessageProcessing {
	m.Payload = slices.Clone(m.Payload)
	if m.Error != nil {
		msg := *m.Error
		m.Error = &msg
	}
	if m.OutboxEntryID != nil {
		id := *m.OutboxEntryID
		m.OutboxEntryID = &id
	}
	return m
}

func cloneRemediation(r remediation.Remediation) remediation.Remediation {
	r.TriggerPayload = slices.Clone(r.TriggerPayload)
	r.TriggerFields = maps.Clone(r.TriggerFields)
	r.LeaseUntil = cloneTimePtr(r.LeaseUntil)
	return r
}

func cloneTry(t remediation.Try) remediation.Try {
	t.FileRevisions = maps.Clone(t.FileRevisions)
	return t
}

func cloneCluster(c remediation.Cluster) remediation.Cluster {
	c.Members = slices.Clone(c.Members)
	if c.Outcome != nil {
		o := *c.Outcome
		c.Outcome = &o
	}
	return c
}

func cloneStep(s remediation.StepRun) remediation.StepRun {
	s.Output = slices.Clone(s.Output)
	s.Findings = slices.Clone(s.Findings)
	s.WaitingOn = slices.Clone(s.WaitingOn)
	s.FinishedAt = cloneTimePtr(s.FinishedAt)
	return s
}

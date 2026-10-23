// Package serialization holds the wire- and persistence-facing DTOs for
// execution-controller's json-tagged boundary types, keeping the domain packages
// (command, model) free of struct tags. It sits outside adapters/ so the
// application layer (service/reports, which writes the task.status.updated:v2
// report payloads to the outbox) may map through it without importing an
// adapter, and outside domain/ so the tags live away from the domain types.
// service/reports and the postgres repository (the deployments columns
// job_params, facts and last_report) map through it, fixing the byte shapes
// here.
package serialization

import (
	"github.com/carolsimone/continuo/execution-controller/domain/command"
)

// DeployTaskDTO is the JSON shape of command.DeployTask as stored in the
// deployments.job_params JSONB column for a production deployment.
type DeployTaskDTO struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	SecretRef      string `json:"secret_ref,omitempty"`
	Attempt        int    `json:"attempt"`
	Operation      string `json:"operation"`
	TimeoutSeconds int64  `json:"timeout_seconds,omitempty"`
}

// DeployTaskFromDomain maps a domain command to its DTO.
func DeployTaskFromDomain(c command.DeployTask) DeployTaskDTO {
	return DeployTaskDTO{
		TaskID:         c.TaskID,
		ScheduleID:     c.ScheduleID,
		ScheduleName:   c.ScheduleName,
		ServiceName:    c.ServiceName,
		SchemaName:     c.SchemaName,
		TableName:      c.TableName,
		JobName:        c.JobName,
		NodeType:       c.NodeType,
		ImageTag:       c.ImageTag,
		SecretRef:      c.SecretRef,
		Attempt:        c.Attempt,
		Operation:      c.Operation,
		TimeoutSeconds: c.TimeoutSeconds,
	}
}

// ToDomain maps a decoded DTO back to the domain command.
func (d DeployTaskDTO) ToDomain() command.DeployTask {
	return command.DeployTask{
		TaskID:         d.TaskID,
		ScheduleID:     d.ScheduleID,
		ScheduleName:   d.ScheduleName,
		ServiceName:    d.ServiceName,
		SchemaName:     d.SchemaName,
		TableName:      d.TableName,
		JobName:        d.JobName,
		NodeType:       d.NodeType,
		ImageTag:       d.ImageTag,
		SecretRef:      d.SecretRef,
		Attempt:        d.Attempt,
		Operation:      d.Operation,
		TimeoutSeconds: d.TimeoutSeconds,
	}
}

// ValidationDeployTaskDTO is the JSON shape of command.ValidationDeployTask as
// stored in the deployments.job_params JSONB column for a validation,
// seed-build, or compile deployment.
type ValidationDeployTaskDTO struct {
	ReleaseID            string   `json:"release_id"`
	NodeID               string   `json:"node_id"`
	ServiceName          string   `json:"service_name"`
	SchemaName           string   `json:"schema_name"`
	TableName            string   `json:"table_name"`
	NodeType             string   `json:"node_type"`
	ImageTag             string   `json:"image_tag"`
	JobName              string   `json:"job_name"`
	CandidateSchema      string   `json:"candidate_schema"`
	CandidateArtifactURI string   `json:"candidate_artifact_uri"`
	ValidationOp         string   `json:"validation_op"`
	ProdSchema           string   `json:"prod_schema"`
	UpstreamNodeIDs      []string `json:"upstream_node_ids"`
	ManifestS3URI        string   `json:"manifest_s3_uri"`
	ParseProdS3URI       string   `json:"parse_prod_s3_uri,omitempty"`
	ParseCandidateS3URI  string   `json:"parse_candidate_s3_uri,omitempty"`
	SourceOverlayURI     string   `json:"source_overlay_uri,omitempty"`
}

// ValidationDeployTaskFromDomain maps a domain command to its DTO.
func ValidationDeployTaskFromDomain(c command.ValidationDeployTask) ValidationDeployTaskDTO {
	return ValidationDeployTaskDTO{
		ReleaseID:            c.ReleaseID,
		NodeID:               c.NodeID,
		ServiceName:          c.ServiceName,
		SchemaName:           c.SchemaName,
		TableName:            c.TableName,
		NodeType:             c.NodeType,
		ImageTag:             c.ImageTag,
		JobName:              c.JobName,
		CandidateSchema:      c.CandidateSchema,
		CandidateArtifactURI: c.CandidateArtifactURI,
		ValidationOp:         c.ValidationOp,
		ProdSchema:           c.ProdSchema,
		UpstreamNodeIDs:      c.UpstreamNodeIDs,
		ManifestS3URI:        c.ManifestS3URI,
		ParseProdS3URI:       c.ParseProdS3URI,
		ParseCandidateS3URI:  c.ParseCandidateS3URI,
		SourceOverlayURI:     c.SourceOverlayURI,
	}
}

// ToDomain maps a decoded DTO back to the domain command.
func (d ValidationDeployTaskDTO) ToDomain() command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID:            d.ReleaseID,
		NodeID:               d.NodeID,
		ServiceName:          d.ServiceName,
		SchemaName:           d.SchemaName,
		TableName:            d.TableName,
		NodeType:             d.NodeType,
		ImageTag:             d.ImageTag,
		JobName:              d.JobName,
		CandidateSchema:      d.CandidateSchema,
		CandidateArtifactURI: d.CandidateArtifactURI,
		ValidationOp:         d.ValidationOp,
		ProdSchema:           d.ProdSchema,
		UpstreamNodeIDs:      d.UpstreamNodeIDs,
		ManifestS3URI:        d.ManifestS3URI,
		ParseProdS3URI:       d.ParseProdS3URI,
		ParseCandidateS3URI:  d.ParseCandidateS3URI,
		SourceOverlayURI:     d.SourceOverlayURI,
	}
}

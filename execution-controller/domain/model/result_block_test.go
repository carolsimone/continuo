package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
)

func TestReasonFromResultBlock(t *testing.T) {
	assert.Equal(t, pkgmodel.FailureReasonImageContractMismatch, model.ReasonFromResultBlock("image_contract_mismatch"))
	assert.Equal(t, pkgmodel.FailureReason(""), model.ReasonFromResultBlock(""))
	assert.Equal(t, pkgmodel.FailureReason(""), model.ReasonFromResultBlock("something_else"))
}

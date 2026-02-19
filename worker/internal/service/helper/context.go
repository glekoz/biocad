package helper

import (
	"context"

	"github.com/glekoz/biocad/worker/pkg/logger"
)

func GetFilenameFromContext(ctx context.Context) string {
	ld, ok := ctx.Value(logger.LogDataKey).(logger.LogData)
	if !ok {
		return "--unknown--file--"
	}
	return ld.Filename
}

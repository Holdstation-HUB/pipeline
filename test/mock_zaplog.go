package test

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"os"
)

func NewMockZapLog() *zap.Logger {
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		os.Stderr,
		zapcore.InfoLevel,
	)

	observed, _ := observer.New(zap.InfoLevel)

	logger := zap.New(zapcore.NewTee(core, observed))

	return logger
}

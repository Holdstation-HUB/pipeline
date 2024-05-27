package core

import (
	"go.uber.org/zap"
)

func WrapRecoverHandle(lggr *zap.Logger, fn func(), onPanic func(interface{})) {
	defer func() {
		if err := recover(); err != nil {
			lggr.Error("error", zap.Any("error", err))

			if onPanic != nil {
				onPanic(err)
			}
		}
	}()
	fn()
}

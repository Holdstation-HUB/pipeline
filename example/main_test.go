//package example
//
//import (
//	"github.com/Holdstation-HUB/pipeline/core"
//	"github.com/Holdstation-HUB/pipeline/test"
//	pkgerrors "github.com/pkg/errors"
//	"testing"
//)
//
//func DefaultGeneratingTask(taskType core.TaskType, ID int, dotID string) (core.Task, error) {
//	var task core.Task
//	switch taskType {
//	case core.TaskTypeHTTP:
//		task = &core.HTTPTask{BaseTask: core.NewBaseTask(id: ID, dotID: dotID)}
//	default:
//		return nil, pkgerrors.Errorf(`unknown task type: "%v"`, taskType)
//	}
//	return task, nil
//}
//
//func TestSomething(t *testing.T) {
//	zapLog := test.NewMockZapLog()
//	runner := core.NewRunner(core.NewDefaultConfig(), zapLog, DefaultGeneratingTask, DefaultConfiguringTask)
//}
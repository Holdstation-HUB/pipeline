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
//		task = &core.HTTPTask{BaseTask: core.NewBaseTask(ID, dotID, nil, nil, 0)}
//	default:
//		return nil, pkgerrors.Errorf(`unknown task type: "%v"`, taskType)
//	}
//	return task, nil
//}
//
//func DefaultConfiguringTask(task core.Task) {
//	switch task.Type() {
//	case core.TaskTypeHTTP:
//		// todo: config for task http
//	case core.TaskTypeQueryDB:
//		// todo: config for task query db
//		//task.(*QueryDBTask).Db = r.Db
//	default:
//	}
//}
//
//func TestSomething(t *testing.T) {
//	zapLog := test.NewMockZapLog()
//	runner := core.NewRunner(core.NewDefaultConfig(), zapLog, DefaultGeneratingTask, DefaultConfiguringTask)
//}
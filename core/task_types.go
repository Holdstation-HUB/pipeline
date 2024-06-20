package core

import (
	"github.com/mitchellh/mapstructure"
	pkgerrors "github.com/pkg/errors"
	cutils "github.com/smartcontractkit/chainlink-common/pkg/utils"
	cnull "github.com/smartcontractkit/chainlink/v2/core/null"
	"reflect"
	"strconv"
	"strings"
)

const (
	TaskTypeAny         TaskType = "any"
	TaskTypeConditional TaskType = "conditional"
	TaskTypeHTTP        TaskType = "http"
	TaskTypeLength      TaskType = "length"
	TaskTypeLessThan    TaskType = "lessthan"
	TaskTypeMerge       TaskType = "merge"
	TaskTypeMultiply    TaskType = "multiply"
	TaskTypeSum         TaskType = "sum"

	// Testing only.
	TaskTypePanic TaskType = "panic"
	TaskTypeMemo  TaskType = "memo"
	TaskTypeFail  TaskType = "fail"

	TaskTypeQueryDB  TaskType = "querydb"
	TaskTypeDecodePK TaskType = "decodepk"
)

var (
	stringType     = reflect.TypeOf("")
	bytesType      = reflect.TypeOf([]byte(nil))
	bytes20Type    = reflect.TypeOf([20]byte{})
	int32Type      = reflect.TypeOf(int32(0))
	nullUint32Type = reflect.TypeOf(cnull.Uint32{})
)

type TaskType string

func (t TaskType) String() string {
	return string(t)
}

// GenerateTask
type GenerateTask func(taskType TaskType, ID int, dotID string) (Task, error)

// ConfigTask
type ConfigTask func(task Task)

func DefaultGeneratingTask(taskType TaskType, ID int, dotID string) (Task, error) {
	var task Task
	switch taskType {
	case TaskTypeHTTP:
		task = &HTTPTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeMemo:
		task = &MemoTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeMerge:
		task = &MergeTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeLength:
		task = &LengthTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeLessThan:
		task = &LessThanTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeConditional:
		task = &ConditionalTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeDecodePK:
		task = &DecodePKTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeQueryDB:
		task = &QueryDBTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeMultiply:
		task = &MultiplyTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	case TaskTypeSum:
		task = &SumTask{BaseTask: BaseTask{id: ID, dotID: dotID}}
	default:
		return nil, pkgerrors.Errorf(`unknown task type: "%v"`, taskType)
	}
	return task, nil
}

func DefaultConfiguringTask(task Task) {
	switch task.Type() {
	case TaskTypeHTTP:
		// todo: config for task http
	case TaskTypeQueryDB:
		// todo: config for task query db
		//task.(*QueryDBTask).Db = r.Db
	default:
	}
}

func UnmarshalTaskFromMap(generatingTask GenerateTask, taskType TaskType, taskMap interface{}, ID int, dotID string) (_ Task, err error) {
	defer cutils.WrapIfError(&err, "UnmarshalTaskFromMap")

	switch taskMap.(type) {
	default:
		return nil, pkgerrors.Errorf("UnmarshalTaskFromMap only accepts a map[string]interface{} or a map[string]string. Got %v (%#v) of type %T", taskMap, taskMap, taskMap)
	case map[string]interface{}, map[string]string:
	}

	taskType = TaskType(strings.ToLower(string(taskType)))

	task, err := generatingTask(taskType, ID, dotID)
	if err != nil {
		return nil, err
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           task,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
				if from != stringType {
					return data, nil
				}
				switch to {
				case nullUint32Type:
					i, err2 := strconv.ParseUint(data.(string), 10, 32)
					return cnull.Uint32From(uint32(i)), err2
				}
				return data, nil
			},
		),
	})
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(taskMap)
	if err != nil {
		return nil, err
	}
	return task, nil
}

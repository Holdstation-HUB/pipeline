package core

import (
	"github.com/mitchellh/mapstructure"
	cmap "github.com/orcaman/concurrent-map/v2"
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

var TaskTypeManager *TaskTypes

type TaskTypes struct {
	TaskTypes   map[string]Task
	TaskTypeMap cmap.ConcurrentMap[TaskType, Task]
}

func Init() {
	TaskTypeMap := cmap.NewWithCustomShardingFunction[TaskType, Task](func(key TaskType) uint32 {
		return uint32(len(key.String()) % 7)
	})

	TaskTypeMap.Set(TaskTypeConditional, &ConditionalTask{})
	TaskTypeMap.Set(TaskTypeHTTP, &HTTPTask{})
	TaskTypeMap.Set(TaskTypeLength, &LengthTask{})
	TaskTypeMap.Set(TaskTypeLessThan, &LessThanTask{})
	TaskTypeMap.Set(TaskTypeMerge, &MergeTask{})
	TaskTypeMap.Set(TaskTypeMultiply, &MultiplyTask{})
	TaskTypeMap.Set(TaskTypeSum, &SumTask{})
	TaskTypeMap.Set(TaskTypeMemo, &MemoTask{})
	TaskTypeMap.Set(TaskTypeQueryDB, &QueryDBTask{})
	TaskTypeMap.Set(TaskTypeDecodePK, &DecodePKTask{})
	TaskTypeManager = &TaskTypes{
		TaskTypeMap: TaskTypeMap,
	}
}

func (t *TaskTypes) AddTaskType(name TaskType, typeA Task) {
	t.TaskTypeMap.SetIfAbsent(name, typeA)
}

func (t *TaskTypes) GetTaskType(name TaskType) Task {
	taskType, ok := t.TaskTypeMap.Get(name)
	if !ok {
		return nil
	}
	return taskType
}

//func UnmarshalTaskFromMapDynamic(taskType TaskType, taskMap interface{}, ID int, dotID string) (_ Task, err error) {
//	defer cutils.WrapIfError(&err, "UnmarshalTaskFromMap")
//
//	switch taskMap.(type) {
//	default:
//		return nil, pkgerrors.Errorf("UnmarshalTaskFromMap only accepts a map[string]interface{} or a map[string]string. Got %v (%#v) of type %T", taskMap, taskMap, taskMap)
//	case map[string]interface{}, map[string]string:
//	}
//
//	taskType = TaskType(strings.ToLower(string(taskType)))
//	task := TaskTypeManager.GetTaskType(taskType)
//	if task == nil {
//		return nil, pkgerrors.Errorf(`unknown task type: "%v"`, taskType)
//	}
//	//baseTask := BaseTask{id: ID, dotID: dotID}
//	type2 := reflect.TypeOf(&task)
//	v := reflect.New(type2)
//	c, ok := v.Interface().(**Task)
//	if !ok {
//		return nil, pkgerrors.Errorf(`task type "%v" is of type %T`, taskType, taskType)
//	}
//	fmt.Print(c)
//	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
//		Result:           task,
//		WeaklyTypedInput: true,
//		DecodeHook: mapstructure.ComposeDecodeHookFunc(
//			mapstructure.StringToTimeDurationHookFunc(),
//			func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
//				if from != stringType {
//					return data, nil
//				}
//				switch to {
//				case nullUint32Type:
//					i, err2 := strconv.ParseUint(data.(string), 10, 32)
//					return cnull.Uint32From(uint32(i)), err2
//				}
//				return data, nil
//			},
//		),
//	})
//	if err != nil {
//		return nil, err
//	}
//
//	err = decoder.Decode(taskMap)
//	if err != nil {
//		return nil, err
//	}
//	return task, nil
//}

func UnmarshalTaskFromMap(taskType TaskType, taskMap interface{}, ID int, dotID string) (_ Task, err error) {
	defer cutils.WrapIfError(&err, "UnmarshalTaskFromMap")

	switch taskMap.(type) {
	default:
		return nil, pkgerrors.Errorf("UnmarshalTaskFromMap only accepts a map[string]interface{} or a map[string]string. Got %v (%#v) of type %T", taskMap, taskMap, taskMap)
	case map[string]interface{}, map[string]string:
	}

	taskType = TaskType(strings.ToLower(string(taskType)))

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

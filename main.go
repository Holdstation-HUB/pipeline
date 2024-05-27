package main

import (
	"context"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"pipeline/core"
	"reflect"
	"time"
)

var specs = `
    min_balance_swap [type="memo" value="2000000000"]
	balance [type="memo" value="1000000000"]
	is_balance_less_than_min_balance [type="lessthan" left="$(balance)" right="$(min_balance_swap)"]
`

func main() {

	shapeMap := cmap.New[Shape]()
	fmt.Printf("type %s\n", reflect.TypeOf(Circle{}))
	shapeMap.Set("circle", &Circle{})

	//var type1 Shape
	type1, _ := shapeMap.Get("circle")
	fmt.Printf("type1 %s\n", type1)

	type2 := reflect.TypeOf(&type1)
	v := reflect.New(type2)
	c := v.Interface().(**Shape)
	fmt.Println(c)

	config := core.NewDefaultConfig()
	//loggr, _ := logger.NewLogger()
	var zapLog *zap.Logger
	var encoderConfig zapcore.EncoderConfig
	var config1 zap.Config

	encoderConfig = zap.NewProductionEncoderConfig()
	config1 = zap.NewProductionConfig()

	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)
	encoderConfig.StacktraceKey = "" // to hide stacktrace info
	config1.EncoderConfig = encoderConfig

	var err error

	zapLog, err = config1.Build(
		zap.AddCallerSkip(1),
		//zap.WrapCore((&apmzap.Core{}).WrapCore)
	)

	if err != nil {
		panic(err)
	}

	core.Init()

	r := core.NewRunner(config, zapLog, nil, nil, nil)
	_, trrs, err := r.ExecuteRun(context.TODO(), core.Spec{
		DotDagSource: specs,
	}, core.NewVarsFrom(nil), zapLog)

	if err != nil {
		fmt.Println(err)
	}
	trrs.FinalResult(zapLog).SingularResult()
	fmt.Println(trrs.FinalResult(zapLog).Values)
}

type Shape interface {
	Name() string
}

type Circle struct {
	r int
}

func (c Circle) Name() string {
	return "circle"
}

type Rectangle struct {
	Circle
}

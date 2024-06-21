package task

import (
	"context"
	"encoding/json"
	"github.com/Holdstation-HUB/pipeline/core"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"net/http"
)

const TaskTypeHTTP core.TaskType = "http"

// Return types:
//
//	string
type HTTPTask struct {
	core.BaseTask                  `mapstructure:",squash"`
	Method                         string
	URL                            string
	RequestData                    string `json:"requestData"`
	AllowUnrestrictedNetworkAccess string
	Headers                        string

	config                 core.Config
	httpClient             *http.Client
	unrestrictedHTTPClient *http.Client
}

var _ core.Task = (*HTTPTask)(nil)

func (t *HTTPTask) Type() core.TaskType {
	return TaskTypeHTTP
}

func (t *HTTPTask) Run(ctx context.Context, lggr *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, -1, -1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}

	var (
		method                         core.StringParam
		url                            core.URLParam
		requestData                    core.MapParam
		allowUnrestrictedNetworkAccess core.BoolParam
		reqHeaders                     core.StringSliceParam
	)
	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&method, core.From(core.NonemptyString(t.Method), "GET")), "method"),
		errors.Wrap(core.ResolveParam(&url, core.From(core.VarExpr(t.URL, vars), core.NonemptyString(t.URL))), "url"),
		errors.Wrap(core.ResolveParam(&requestData, core.From(core.VarExpr(t.RequestData, vars), core.JSONWithVarExprs(t.RequestData, vars, false), nil)), "requestData"),
		// Any hardcoded strings used for URL uses the unrestricted HTTP adapter
		// Interpolated variable URLs use restricted HTTP adapter by default
		// You must set allowUnrestrictedNetworkAccess=true on the task to enable variable-interpolated URLs to make restricted network requests
		errors.Wrap(core.ResolveParam(&allowUnrestrictedNetworkAccess, core.From(core.NonemptyString(t.AllowUnrestrictedNetworkAccess), !core.VariableRegexp.MatchString(t.URL))), "allowUnrestrictedNetworkAccess"),
		errors.Wrap(core.ResolveParam(&reqHeaders, core.From(core.NonemptyString(t.Headers), "[]")), "reqHeaders"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	if len(reqHeaders)%2 != 0 {
		return core.Result{Error: errors.Errorf("headers must have an even number of elements")}, runInfo
	}

	requestDataJSON, err := json.Marshal(requestData)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}
	lggr.Debug("HTTP task: sending request",
		zap.String("requestData", string(requestDataJSON)),
		zap.String("url", url.String()),
		zap.Any("method", method),
		zap.Any("reqHeaders", reqHeaders),
		zap.Any("allowUnrestrictedNetworkAccess", allowUnrestrictedNetworkAccess),
	)

	requestCtx, cancel := core.HttpRequestCtx(ctx, t, t.config)
	defer cancel()

	var client *http.Client
	if allowUnrestrictedNetworkAccess {
		client = t.unrestrictedHTTPClient
	} else {
		client = t.httpClient
	}
	responseBytes, statusCode, respHeaders, _, err := core.MakeHTTPRequest(requestCtx, lggr, method, url, reqHeaders, requestData, client, t.config.DefaultHTTPLimit())
	if err != nil {
		if errors.Is(errors.Cause(err), core.ErrDisallowedIP) {
			err = errors.Wrap(err, `connections to local resources are disabled by default, if you are sure this is safe, you can enable on a per-task basis by setting allowUnrestrictedNetworkAccess="true" in the pipeline task spec, e.g. fetch [type="http" method=GET url="$(decode_cbor.url)" allowUnrestrictedNetworkAccess="true"]`)
		}
		return core.Result{Error: err}, core.RunInfo{IsRetryable: core.IsRetryableHTTPError(statusCode, err)}
	}

	lggr.Debug("HTTP task got response",
		zap.String("response", string(responseBytes)),
		zap.Any("respHeaders", respHeaders),
		zap.String("url", url.String()),
		zap.String("dotID", t.DotID()),
	)

	// NOTE: We always stringify the response since this is required for all current jobs.
	// If a binary response is required we might consider adding an adapter
	// flag such as  "BinaryMode: true" which passes through raw binary as the
	// value instead.
	return core.Result{Value: string(responseBytes)}, runInfo
}

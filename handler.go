package code

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
)

const InternalServerError = "Internal Server Error"

type CodeHandler struct {
	Loader         CodeLoader
	Resource       string
	Action         string
	RequiredMaster bool
	LogError       func(context.Context, string)
	LogWriter      LogWriter
}

func NewDefaultCodeHandler(loader CodeLoader, resource string, action string, logError func(context.Context, string), logWriter LogWriter) *CodeHandler {
	return NewCodeHandler(loader, resource, action, true, logError, logWriter)
}
func NewCodeHandler(loader CodeLoader, resource string, action string, requiredMaster bool, logError func(context.Context, string), logWriter LogWriter) *CodeHandler {
	if len(resource) == 0 {
		resource = "code"
	}
	if len(action) == 0 {
		action = "load"
	}
	h := CodeHandler{Loader: loader, Resource: resource, Action: action, RequiredMaster: requiredMaster, LogWriter: logWriter, LogError: logError}
	return &h
}
func (c *CodeHandler) Load(w http.ResponseWriter, r *http.Request) {
	code := ""
	if c.RequiredMaster {
		if r.Method == "GET" {
			i := strings.LastIndex(r.RequestURI, "/")
			if i >= 0 {
				code = r.RequestURI[i+1:]
			}
		} else {
			b, er1 := ioutil.ReadAll(r.Body)
			if er1 != nil {
				respondString(w, r, http.StatusBadRequest, "Body cannot is empty")
				return
			}
			code = strings.Trim(string(b), " ")
		}
	}
	result, er4 := c.Loader.Load(r.Context(), code)
	if er4 != nil {
		respondError(w, r, http.StatusInternalServerError, InternalServerError, c.LogError, c.Resource, c.Action, er4, c.LogWriter)
	} else {
		succeed(w, r, http.StatusOK, result, c.LogWriter, c.Resource, c.Action)
	}
}

type LogWriter interface {
	Write(ctx context.Context, resource string, action string, success bool, desc string) error
}

func respondString(w http.ResponseWriter, r *http.Request, code int, result string) {
	w.WriteHeader(code)
	w.Write([]byte(result))
}
func respond(w http.ResponseWriter, r *http.Request, code int, result interface{}, logWriter LogWriter, resource string, action string, success bool, desc string) {
	response, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
	if logWriter != nil {
		logWriter.Write(r.Context(), resource, action, success, desc)
	}
}
func respondError(w http.ResponseWriter, r *http.Request, code int, result interface{}, logError func(context.Context, string), resource string, action string, err error, logWriter LogWriter) {
	if logError != nil {
		logError(r.Context(), err.Error())
	}
	respond(w, r, code, result, logWriter, resource, action, false, err.Error())
}
func succeed(w http.ResponseWriter, r *http.Request, code int, result interface{}, logWriter LogWriter, resource string, action string) {
	respond(w, r, code, result, logWriter, resource, action, true, "")
}

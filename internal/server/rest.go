// Copyright 2021-2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/processor"
	"github.com/lf-edge/ekuiper/internal/server/middleware"
	"github.com/lf-edge/ekuiper/internal/topo/planner"
	"github.com/lf-edge/ekuiper/internal/trial"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/hidden"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"github.com/lf-edge/ekuiper/pkg/memory"
)

const (
	ContentType     = "Content-Type"
	ContentTypeJSON = "application/json"
)

var (
	uploadDir       string
	uploadsDb       kv.KeyValue
	uploadsStatusDb kv.KeyValue
)

type statementDescriptor struct {
	Sql string `json:"sql,omitempty"`
}

func decodeStatementDescriptor(reader io.ReadCloser) (statementDescriptor, error) {
	sd := statementDescriptor{}
	err := json.NewDecoder(reader).Decode(&sd)
	// Problems decoding
	if err != nil {
		return sd, fmt.Errorf("Error decoding the statement descriptor: %v", err)
	}
	return sd, nil
}

// Handle applies the specified error and error concept to the HTTP response writer
func handleError(w http.ResponseWriter, err error, prefix string, logger api.Logger) {
	message := prefix
	if message != "" {
		message += ": "
	}
	message += err.Error()
	logger.Error(message)
	var ec int
	switch e := err.(type) {
	case *errorx.Error:
		switch e.Code() {
		case errorx.NOT_FOUND:
			ec = http.StatusNotFound
		default:
			ec = http.StatusBadRequest
		}
	default:
		ec = http.StatusBadRequest
	}

	http.Error(w, packageInternalErrorCode(err, message), ec)
}

func packageInternalErrorCode(err error, msg string) string {
	errCode := errorx.Undefined_Err
	if errWithCode, ok := err.(errorx.ErrorWithCode); ok {
		errCode = errWithCode.Code()
	}
	return fmt.Sprintf(`{"error":%v,"message":"%v"}`, errCode, msg)
}

func jsonResponse(i interface{}, w http.ResponseWriter, logger api.Logger) {
	w.Header().Add(ContentType, ContentTypeJSON)

	jsonByte, err := json.Marshal(i)
	if err != nil {
		handleError(w, err, "", logger)
	}
	w.Header().Add("Content-Length", strconv.Itoa(len(jsonByte)))

	_, err = w.Write(jsonByte)
	// Problems encoding
	if err != nil {
		handleError(w, err, "", logger)
	}
}

func jsonByteResponse(buffer bytes.Buffer, w http.ResponseWriter, logger api.Logger) {
	w.Header().Add(ContentType, ContentTypeJSON)

	w.Header().Add("Content-Length", strconv.Itoa(buffer.Len()))

	_, err := w.Write(buffer.Bytes())
	// Problems encoding
	if err != nil {
		handleError(w, err, "", logger)
	}
}

func createRestServer(ip string, port int, needToken bool) *http.Server {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		panic(err)
	}
	uploadDir = filepath.Join(dataDir, "uploads")
	uploadsDb, err = store.GetKV("uploads")
	if err != nil {
		panic(err)
	}
	uploadsStatusDb, err = store.GetKV("uploadsStatusDb")
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/stop", stopHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/ping", pingHandler).Methods(http.MethodGet)
	r.HandleFunc("/streams", streamsHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/streamdetails", streamDetailsHandler).Methods(http.MethodGet)
	r.HandleFunc("/streams/{name}", streamHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/streams/{name}/schema", streamSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/tables", tablesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/tabledetails", tableDetailsHandler).Methods(http.MethodGet)
	r.HandleFunc("/tables/{name}", tableHandler).Methods(http.MethodGet, http.MethodDelete, http.MethodPut)
	r.HandleFunc("/tables/{name}/schema", tableSchemaHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules", rulesHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/rules/{name}", ruleHandler).Methods(http.MethodDelete, http.MethodGet, http.MethodPut)
	r.HandleFunc("/rules/status/all", getAllRuleStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/status", getStatusRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/{name}/start", startRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/stop", stopRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/restart", restartRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/topo", getTopoRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/rules/validate", validateRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules/{name}/reset_state", ruleStateHandler).Methods(http.MethodPut)
	r.HandleFunc("/rules/{name}/explain", explainRuleHandler).Methods(http.MethodGet)
	r.HandleFunc("/ruletest", testRuleHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}/start", testRuleStartHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruletest/{name}", testRuleStopHandler).Methods(http.MethodDelete)
	r.HandleFunc("/ruleset/export", exportHandler).Methods(http.MethodPost)
	r.HandleFunc("/ruleset/import", importHandler).Methods(http.MethodPost)
	r.HandleFunc("/configs", configurationUpdateHandler).Methods(http.MethodPatch)
	r.HandleFunc("/config/uploads", fileUploadHandler).Methods(http.MethodPost, http.MethodGet)
	r.HandleFunc("/config/uploads/{name}", fileDeleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/data/export", configurationExportHandler).Methods(http.MethodGet, http.MethodPost)
	r.HandleFunc("/data/import", configurationImportHandler).Methods(http.MethodPost)
	r.HandleFunc("/data/import/status", configurationStatusHandler).Methods(http.MethodGet)
	r.HandleFunc("/connection/websocket", connectionHandler).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
	r.HandleFunc("/async/data/import", registerDataImportTask).Methods(http.MethodPost)
	r.HandleFunc("/async/data/{id}", queryAsyncTaskStatus).Methods(http.MethodGet)
	r.HandleFunc("/async/task/{id}/cancel", asyncTaskCancelHandler).Methods(http.MethodPost)
	// Register extended routes
	for k, v := range components {
		logger.Infof("register rest endpoint for component %s", k)
		v.rest(r)
	}

	if needToken {
		r.Use(middleware.Auth)
	}

	server := &http.Server{
		Addr: cast.JoinHostPortInt(ip, port),
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 60 * 5,
		ReadTimeout:  time.Second * 60 * 5,
		IdleTimeout:  time.Second * 60,
		Handler:      handlers.CORS(handlers.AllowedHeaders([]string{"Accept", "Accept-Language", "Content-Type", "Content-Language", "Origin", "Authorization"}), handlers.AllowedMethods([]string{"POST", "GET", "PUT", "DELETE", "HEAD"}))(r),
	}
	server.SetKeepAlivesEnabled(false)
	return server
}

type fileContent struct {
	Name     string `json:"name"`
	Content  string `json:"content"`
	FilePath string `json:"file"`
}

func (f *fileContent) InstallScript() string {
	marshal, err := json.Marshal(f)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func (f *fileContent) Validate() error {
	if f.Content == "" && f.FilePath == "" {
		return fmt.Errorf("invalid body: content or FilePath is required")
	}
	if f.Name == "" {
		return fmt.Errorf("invalid body: name is required")
	}
	return nil
}

func upload(file *fileContent) error {
	err := getFile(file)
	if err != nil {
		_ = uploadsStatusDb.Set(file.Name, err.Error())
		return err
	}

	return uploadsDb.Set(file.Name, file.InstallScript())
}

func getFile(file *fileContent) error {
	filePath := filepath.Join(uploadDir, file.Name)
	dst, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer dst.Close()

	if file.FilePath != "" {
		err := httpx.DownloadFile(filePath, file.FilePath)
		if err != nil {
			return err
		}
	} else {
		_, err := dst.Write([]byte(file.Content))
		if err != nil {
			return err
		}
	}
	return nil
}

func explainRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	// fetch the rule which will be explained
	rule, err := ruleProcessor.GetRuleById(name)
	if err != nil {
		handleError(w, err, "explain rules error", logger)
		return
	}
	if rule == nil {
		handleError(w, errorx.NewWithCode(errorx.NOT_FOUND, "rule not found"), "", logger)
		return
	}
	if rule.Sql == "" {
		handleError(w, errors.New("only support explain sql now"), "explain rules error", logger)
		return
	}
	var explainInfo string
	explainInfo, err = planner.GetExplainInfoFromLogicalPlan(rule)
	if err != nil {
		handleError(w, err, "explain rules error", logger)
		return
	}
	// resp := planner.BuildExplainResultFromLp(lp, 0)
	w.Write([]byte(explainInfo))
}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	// Upload or overwrite a file
	case http.MethodPost:
		switch r.Header.Get("Content-Type") {
		case "application/json":
			fc := &fileContent{}
			defer r.Body.Close()
			err := json.NewDecoder(r.Body).Decode(fc)
			if err != nil {
				handleError(w, err, "Invalid body: Error decoding file json", logger)
				return
			}
			err = fc.Validate()
			if err != nil {
				handleError(w, err, "Invalid body: missing necessary field", logger)
				return
			}

			filePath := filepath.Join(uploadDir, fc.Name)
			err = upload(fc)
			if err != nil {
				handleError(w, err, "Upload error: getFile has error", logger)
				return
			}
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(filePath))
		default:
			// Maximum upload of 1 GB files
			err := r.ParseMultipartForm(1024 << 20)
			if err != nil {
				handleError(w, err, "Error parse the multi part form", logger)
				return
			}

			// Get handler for filename, size and headers
			file, handler, err := r.FormFile("uploadFile")
			if err != nil {
				handleError(w, err, "Error Retrieving the File", logger)
				return
			}

			defer file.Close()

			// Create file
			filePath := filepath.Join(uploadDir, handler.Filename)
			dst, err := os.Create(filePath)
			defer dst.Close()
			if err != nil {
				handleError(w, err, "Error creating the file", logger)
				return
			}

			// Copy the uploaded file to the created file on the filesystem
			if _, err := io.Copy(dst, file); err != nil {
				handleError(w, err, "Error writing the file", logger)
				return
			}

			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(filePath))
		}

	case http.MethodGet:
		// Get the list of files in the upload directory
		files, err := os.ReadDir(uploadDir)
		if err != nil {
			handleError(w, err, "Error reading the file upload dir", logger)
			return
		}
		fileNames := make([]string, len(files))
		for i, f := range files {
			fileNames[i] = filepath.Join(uploadDir, f.Name())
		}
		jsonResponse(fileNames, w, logger)
	}
}

func fileDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	filePath := filepath.Join(uploadDir, name)
	e := os.Remove(filePath)
	if e != nil {
		handleError(w, e, "Error deleting the file", logger)
		return
	}
	_ = uploadsDb.Delete(name)
	_ = uploadsStatusDb.Delete(name)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

type information struct {
	Version       string `json:"version"`
	Os            string `json:"os"`
	Arch          string `json:"arch"`
	UpTimeSeconds int64  `json:"upTimeSeconds"`
	CpuUsage      string `json:"cpuUsage,omitempty"`
	MemoryUsed    string `json:"memoryUsed,omitempty"`
	MemoryTotal   string `json:"memoryTotal"`
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet, http.MethodPost:
		stopEKuiper()
		w.Write([]byte("stop success"))
	}
}

// The handler for root
func rootHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet, http.MethodPost:
		w.WriteHeader(http.StatusOK)
		info := new(information)
		info.Version = version
		info.UpTimeSeconds = time.Now().Unix() - startTimeStamp
		info.Os = runtime.GOOS
		info.Arch = runtime.GOARCH
		if sysMetrics != nil {
			info.CpuUsage = sysMetrics.GetCpuUsage()
			info.MemoryUsed = sysMetrics.GetMemoryUsage()
		}
		info.MemoryTotal = fmt.Sprintf("%d", memory.GetMemoryTotal())
		byteInfo, _ := json.Marshal(info)
		w.Write(byteInfo)
	}
}

func pingHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func sourceDetailsManageHandler(w http.ResponseWriter, r *http.Request, st ast.StreamType) {
	defer r.Body.Close()
	var (
		content []processor.StreamDetail
		err     error
		kind    string
	)
	if st == ast.TypeTable {
		kind = r.URL.Query().Get("kind")
		if kind == "scan" {
			kind = ast.StreamKindScan
		} else if kind == "lookup" {
			kind = ast.StreamKindLookup
		} else {
			kind = ""
		}
	}
	content, err = streamProcessor.ShowStreamOrTableDetails(kind, st)
	if err != nil {
		handleError(w, err, fmt.Sprintf("%s command error", cases.Title(language.Und).String(ast.StreamTypeMap[st])), logger)
		return
	}
	jsonResponse(content, w, logger)
}

func sourcesManageHandler(w http.ResponseWriter, r *http.Request, st ast.StreamType) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodGet:
		var (
			content []string
			err     error
			kind    string
		)
		if st == ast.TypeTable {
			kind = r.URL.Query().Get("kind")
			if kind == "scan" {
				kind = ast.StreamKindScan
			} else if kind == "lookup" {
				kind = ast.StreamKindLookup
			} else {
				kind = ""
			}
		}
		if kind != "" {
			content, err = streamProcessor.ShowTable(kind)
		} else {
			content, err = streamProcessor.ShowStream(st)
		}
		if err != nil {
			handleError(w, err, fmt.Sprintf("%s command error", cases.Title(language.Und).String(ast.StreamTypeMap[st])), logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodPost:
		v, err := decodeStatementDescriptor(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		content, err := streamProcessor.ExecStreamSql(v.Sql)
		if err != nil {
			handleError(w, err, fmt.Sprintf("%s command error", cases.Title(language.Und).String(ast.StreamTypeMap[st])), logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(content))
	}
}

func sourceManageHandler(w http.ResponseWriter, r *http.Request, st ast.StreamType) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	switch r.Method {
	case http.MethodGet:
		content, err := streamProcessor.DescStream(name, st)
		if err != nil {
			handleError(w, err, fmt.Sprintf("describe %s error", ast.StreamTypeMap[st]), logger)
			return
		}
		jsonResponse(content, w, logger)
	case http.MethodDelete:
		content, err := streamProcessor.DropStream(name, st)
		if err != nil {
			handleError(w, err, fmt.Sprintf("delete %s error", ast.StreamTypeMap[st]), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	case http.MethodPut:
		v, err := decodeStatementDescriptor(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		content, err := streamProcessor.ExecReplaceStream(name, v.Sql, st)
		if err != nil {
			handleError(w, err, fmt.Sprintf("%s command error", cases.Title(language.Und).String(ast.StreamTypeMap[st])), logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	}
}

// list or create streams
func streamDetailsHandler(w http.ResponseWriter, r *http.Request) {
	sourceDetailsManageHandler(w, r, ast.TypeStream)
}

// list or create streams
func streamsHandler(w http.ResponseWriter, r *http.Request) {
	sourcesManageHandler(w, r, ast.TypeStream)
}

// describe or delete a stream
func streamHandler(w http.ResponseWriter, r *http.Request) {
	sourceManageHandler(w, r, ast.TypeStream)
}

// list or create streams
func tableDetailsHandler(w http.ResponseWriter, r *http.Request) {
	sourceDetailsManageHandler(w, r, ast.TypeTable)
}

// list or create tables
func tablesHandler(w http.ResponseWriter, r *http.Request) {
	sourcesManageHandler(w, r, ast.TypeTable)
}

func tableHandler(w http.ResponseWriter, r *http.Request) {
	sourceManageHandler(w, r, ast.TypeTable)
}

func streamSchemaHandler(w http.ResponseWriter, r *http.Request) {
	sourceSchemaHandler(w, r, ast.TypeStream)
}

func tableSchemaHandler(w http.ResponseWriter, r *http.Request) {
	sourceSchemaHandler(w, r, ast.TypeTable)
}

func sourceSchemaHandler(w http.ResponseWriter, r *http.Request, st ast.StreamType) {
	vars := mux.Vars(r)
	name := vars["name"]
	content, err := streamProcessor.GetInferredJsonSchema(name, st)
	if err != nil {
		handleError(w, err, fmt.Sprintf("get schema of %s error", ast.StreamTypeMap[st]), logger)
		return
	}
	jsonResponse(content, w, logger)
}

// list or create rules
func rulesHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	switch r.Method {
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		id, err := createRule("", string(body))
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "Rule %s was created successfully.", id)
	case http.MethodGet:
		content, err := getAllRulesWithStatus()
		if err != nil {
			handleError(w, err, "Show rules error", logger)
			return
		}
		jsonResponse(content, w, logger)
	}
}

// describe or delete a rule
func ruleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	switch r.Method {
	case http.MethodGet:
		rule, err := ruleProcessor.GetRuleJson(name)
		if err != nil {
			handleError(w, err, "Describe rule error", logger)
			return
		}
		if hidden.IsHiddenNecessary(rule) {
			m := make(map[string]interface{})
			if err := json.Unmarshal([]byte(rule), &m); err != nil {
				handleError(w, err, "Describe rule error", logger)
				return
			}
			bs, err := json.Marshal(hidden.HiddenPassword(m))
			if err != nil {
				handleError(w, err, "Describe rule error", logger)
				return
			}
			w.Header().Add(ContentType, ContentTypeJSON)
			w.Write(bs)
			return
		} else {
			w.Header().Add(ContentType, ContentTypeJSON)
			w.Write([]byte(rule))
		}
	case http.MethodDelete:
		deleteRule(name)
		content, err := ruleProcessor.ExecDrop(name)
		if err != nil {
			handleError(w, err, "Delete rule error", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	case http.MethodPut:
		_, err := ruleProcessor.GetRuleById(name)
		if err != nil {
			handleError(w, err, "Rule not found", logger)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		newRuleJson, err := replaceRulePassword(name, string(body))
		if err != nil {
			handleError(w, err, "Invalid body", logger)
			return
		}
		err = updateRule(name, newRuleJson, true)
		if err != nil {
			handleError(w, err, "Update rule error", logger)
			return
		}
		// Update to db after validation
		_, err = ruleProcessor.ExecUpdate(name, newRuleJson)
		if err != nil {
			handleError(w, err, "Update rule error, suggest to delete it and recreate", logger)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Rule %s was updated successfully.", name)
	}
}

func getAllRuleStatusHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	s, err := getAllRuleStatus()
	if err != nil {
		handleError(w, err, "get rules status error", logger)
		return
	}
	w.Header().Set(ContentType, ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(s))
}

// get status of a rule
func getStatusRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	content, err := getRuleStatus(name)
	if err != nil {
		handleError(w, err, "get rule status error", logger)
		return
	}
	w.Header().Set(ContentType, ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(content))
}

// start a rule
func startRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	err := startRule(name)
	if err != nil {
		handleError(w, err, "start rule error", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Rule %s was started", name)
}

// stop a rule
func stopRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	result, err := stopRule(name)
	if err != nil {
		handleError(w, err, "stop rule error", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(result))
}

// restart a rule
func restartRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	err := restartRule(name)
	if err != nil {
		handleError(w, err, "restart rule error", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Rule %s was restarted", name)
}

// get topo of a rule
func getTopoRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	name := vars["name"]

	content, err := getRuleTopo(name)
	if err != nil {
		handleError(w, err, "get rule topo error", logger)
		return
	}
	w.Header().Set(ContentType, ContentTypeJSON)
	w.Write([]byte(content))
}

// validate a rule
func validateRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	sources, validate, err := validateRule("", string(body))
	if !validate {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(err.Error()))
		return
	}
	resp := make(map[string]interface{})
	resp["valid"] = validate
	resp["sources"] = sources
	bs, _ := json.Marshal(resp)
	w.WriteHeader(http.StatusOK)
	w.Write(bs)
}

type rulesetInfo struct {
	Content  string `json:"content"`
	FilePath string `json:"file"`
}

func importHandler(w http.ResponseWriter, r *http.Request) {
	rsi := &rulesetInfo{}
	err := json.NewDecoder(r.Body).Decode(rsi)
	if err != nil {
		handleError(w, err, "Invalid body: Error decoding json", logger)
		return
	}
	if rsi.Content != "" && rsi.FilePath != "" {
		handleError(w, errors.New("bad request"), "Invalid body: Cannot specify both content and file", logger)
		return
	} else if rsi.Content == "" && rsi.FilePath == "" {
		handleError(w, errors.New("bad request"), "Invalid body: must specify content or file", logger)
		return
	}
	content := []byte(rsi.Content)
	if rsi.FilePath != "" {
		reader, err := httpx.ReadFile(rsi.FilePath)
		if err != nil {
			handleError(w, err, "Fail to read file", logger)
			return
		}
		defer reader.Close()
		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, reader)
		if err != nil {
			handleError(w, err, "fail to convert file", logger)
			return
		}
		content = buf.Bytes()
	}
	rules, counts, err := rulesetProcessor.Import(content)
	if err != nil {
		handleError(w, nil, "Import ruleset error", logger)
		return
	}
	infra.SafeRun(func() error {
		for _, name := range rules {
			rul, ee := ruleProcessor.GetRuleById(name)
			if ee != nil {
				logger.Error(ee)
				continue
			}
			reply := recoverRule(rul)
			if reply != "" {
				logger.Error(reply)
			}
		}
		return nil
	})
	fmt.Fprintf(w, "imported %d streams, %d tables and %d rules", counts[0], counts[1], counts[2])
}

func exportHandler(w http.ResponseWriter, r *http.Request) {
	const name = "ekuiper_export.json"
	exported, _, err := rulesetProcessor.Export()
	if err != nil {
		handleError(w, err, "export error", logger)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Disposition", "Attachment")
	http.ServeContent(w, r, name, time.Now(), exported)
}

func testRuleHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(w, err, "Invalid body", logger)
		return
	}
	id, err := trial.TrialManager.CreateRule(string(body))
	if err != nil {
		handleError(w, err, "", logger)
		return
	}
	result := map[string]any{
		"id":   id,
		"port": conf.Config.Source.HttpServerPort,
	}
	jsonResponse(result, w, logger)
}

func testRuleStartHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	id := vars["name"]
	err := trial.TrialManager.StartRule(id)
	if err != nil {
		handleError(w, err, "start rule error", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Test rule %s was started", id)
}

func testRuleStopHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	id := vars["name"]
	trial.TrialManager.StopRule(id)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Test rule %s was stopped", id)
}

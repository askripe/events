package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/devopsext/events/common"
	sreCommon "github.com/devopsext/sre/common"
)

type ChangeMonitorProcessor struct {
	outputs  *common.Outputs
	tracer   sreCommon.Tracer
	logger   sreCommon.Logger
	requests sreCommon.Counter
	errors   sreCommon.Counter
}

//type T struct {
//	Id          int           `json:"id"`
//	Type        string        `json:"type"`
//	CreatedBy   string        `json:"createdBy"`
//	CreatedAt   time.Time     `json:"createdAt"`
//	Data        interface{}   `json:"data,omitempty"`
//	PreData     interface{}   `json:"preData,omitempty"`
//	Tags        []interface{} `json:"tags,omitempty"`
//	FeatureName string        `json:"featureName"`
//	Project     string        `json:"project"`
//	Environment interface{}   `json:"environment,omitempty"`
//}

type ChangeMonitorRequest struct {
	Id      int `json:"id"`
	Service struct {
		Name string `json:"name"`
		Id   int    `json:"id"`
	} `json:"service"`
	Created        time.Time   `json:"created"`
	Source         string      `json:"source"`
	PipelineStatus string      `json:"pipeline_status"`
	DeployType     string      `json:"deploy_type"`
	EnrichStatus   string      `json:"enrich_status"`
	ExceptionText  interface{} `json:"exception_text"`
	EnrichedData   struct {
		User    string `json:"user"`
		History struct {
			FullDeploy struct {
				End    int64  `json:"end"`
				Start  int64  `json:"start"`
				Status string `json:"status"`
			} `json:"full_deploy"`
			CanaryInstall struct {
				End    int64  `json:"end"`
				Start  int64  `json:"start"`
				Status string `json:"status"`
			} `json:"canary_install"`
			CanaryUninstall struct {
				End    int64  `json:"end"`
				Start  int64  `json:"start"`
				Status string `json:"status"`
			} `json:"canary_uninstall"`
			ComponentDeploy struct {
				End    int64  `json:"end"`
				Start  int64  `json:"start"`
				Status string `json:"status"`
			} `json:"component_deploy"`
		} `json:"history"`
		Version   string `json:"version"`
		EndTime   int64  `json:"end_time"`
		OtherDiff []struct {
			Diff string `json:"diff"`
		} `json:"other_diff"`
		StartTime      int64       `json:"start_time"`
		DeployType     string      `json:"deploy_type"`
		JiraTicket     string      `json:"jira_ticket"`
		PipelineId     string      `json:"pipeline_id"`
		ServiceName    string      `json:"service_name"`
		DeployAction   string      `json:"deploy_action"`
		DeployStatus   string      `json:"deploy_status"`
		PipelineName   string      `json:"pipeline_name"`
		RevisionLink   string      `json:"revision_link"`
		PipelineStatus string      `json:"pipeline_status"`
		PropertiesDiff interface{} `json:"properties_diff"`
	} `json:"enriched_data"`
	PipelineId   string      `json:"pipeline_id"`
	Version      string      `json:"version"`
	JiraTicket   string      `json:"jira_ticket"`
	User         string      `json:"user"`
	IsHealthy    interface{} `json:"is_healthy"`
	LifeTime     interface{} `json:"life_time"`
	DeployStatus string      `json:"deploy_status"`
	Comment      interface{} `json:"comment"`
	Details      interface{} `json:"details"`
	History      struct {
		FullDeploy struct {
			End    int64  `json:"end"`
			Start  int64  `json:"start"`
			Status string `json:"status"`
		} `json:"full_deploy"`
		CanaryInstall struct {
			End    int64  `json:"end"`
			Start  int64  `json:"start"`
			Status string `json:"status"`
		} `json:"canary_install"`
		CanaryUninstall struct {
			End    int64  `json:"end"`
			Start  int64  `json:"start"`
			Status string `json:"status"`
		} `json:"canary_uninstall"`
		ComponentDeploy struct {
			End    int64  `json:"end"`
			Start  int64  `json:"start"`
			Status string `json:"status"`
		} `json:"component_deploy"`
	} `json:"history"`
}
type ChangeMonitorResponse struct {
	Message string
}

func ChangeMonitorProcessorType() string {
	return "ChangeMonitor"
}

func (p *ChangeMonitorProcessor) EventType() string {
	return common.AsEventType(ChangeMonitorProcessorType())
}

func (p *ChangeMonitorProcessor) send(span sreCommon.TracerSpan, channel string, o interface{}, t *time.Time) {

	e := &common.Event{
		Channel: channel,
		Type:    p.EventType(),
		Data:    o,
	}
	if t != nil && (*t).UnixNano() > 0 {
		e.SetTime((*t).UTC())
	} else {
		e.SetTime(time.Now().UTC())
	}
	if span != nil {
		e.SetSpanContext(span.GetContext())
		e.SetLogger(p.logger)
	}
	p.outputs.Send(e)
}

func (p *ChangeMonitorProcessor) HandleEvent(e *common.Event) error {

	if e == nil {
		p.logger.Debug("Event is not defined")
		return nil
	}
	p.requests.Inc(e.Channel)
	p.outputs.Send(e)
	return nil
}

func (p *ChangeMonitorProcessor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) error {

	span := p.tracer.StartChildSpan(r.Header)
	defer span.Finish()

	channel := strings.TrimLeft(r.URL.Path, "/")
	p.requests.Inc(channel)

	var body []byte
	if r.Body != nil {
		if data, err := io.ReadAll(r.Body); err == nil {
			body = data
		}
	}

	if len(body) == 0 {
		p.errors.Inc(channel)
		err := errors.New("empty body")
		p.logger.SpanError(span, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	p.logger.SpanDebug(span, "Body => %s", body)

	var event ChangeMonitorRequest
	if err := json.Unmarshal(body, &event); err != nil {
		p.errors.Inc(channel)
		p.logger.SpanError(span, err)
		http.Error(w, "Error unmarshaling message", http.StatusInternalServerError)
		return err
	}
	timestamp := time.Unix(event.EnrichedData.EndTime/1000, 0)
	p.send(span, channel, event, &timestamp)

	response := &DataDogResponse{
		Message: "OK",
	}

	resp, err := json.Marshal(response)
	if err != nil {
		p.errors.Inc(channel)
		p.logger.SpanError(span, "Can't encode response: %v", err)
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
		return err
	}

	if _, err := w.Write(resp); err != nil {
		p.errors.Inc(channel)
		p.logger.SpanError(span, "Can't write response: %v", err)
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
		return err
	}
	return nil
}

func NewChangeMonitorProcessor(outputs *common.Outputs, observability *common.Observability) *ChangeMonitorProcessor {

	return &ChangeMonitorProcessor{
		outputs:  outputs,
		logger:   observability.Logs(),
		tracer:   observability.Traces(),
		requests: observability.Metrics().Counter("requests", "Count of all changemonitor processor requests", []string{"channel"}, "changemonitor", "processor"),
		errors:   observability.Metrics().Counter("errors", "Count of all changemonitor processor errors", []string{"channel"}, "changemonitor", "processor"),
	}
}

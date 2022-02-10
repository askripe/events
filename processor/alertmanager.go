package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/devopsext/events/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/prometheus/alertmanager/template"
	"github.com/tidwall/gjson"
)

type AlertmanagerProcessor struct {
	outputs *common.Outputs
	tracer  sreCommon.Tracer
	logger  sreCommon.Logger
	counter sreCommon.Counter
}

type AlertmanagerResponse struct {
	Message string
}

func AlertmanagerProcessorType() string {
	return "Alertmanager"
}

func (p *AlertmanagerProcessor) EventType() string {
	return common.AsEventType(AlertmanagerProcessorType())
}

func (p *AlertmanagerProcessor) prepareStatus(status string) string {
	return strings.Title(strings.ToLower(status))
}

func (p *AlertmanagerProcessor) send(span sreCommon.TracerSpan, channel string, data *template.Data) {

	for _, alert := range data.Alerts {

		status := p.prepareStatus(alert.Status)
		e := &common.Event{
			Channel: channel,
			Type:    p.EventType(),
			Data:    alert,
		}
		e.SetTime(alert.StartsAt.UTC())
		if span != nil {
			e.SetSpanContext(span.GetContext())
			e.SetLogger(p.logger)
		}
		p.outputs.Send(e)
		p.counter.Inc(status, channel)
	}
}

func (p *AlertmanagerProcessor) HandleEvent(e *common.Event) {

	if e == nil {
		p.logger.Debug("Event is not defined")
		return
	}

	json, err := e.JsonBytes()
	if err != nil {
		p.logger.Error(err)
		return
	}

	status := gjson.GetBytes(json, "data.status").String()

	p.outputs.Send(e)
	p.counter.Inc(status, e.Channel)
}

func (p *AlertmanagerProcessor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) {

	span := p.tracer.StartChildSpan(r.Header)
	defer span.Finish()

	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}

	if len(body) == 0 {

		err := errors.New("Empty body")
		p.logger.SpanError(span, err)
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	p.logger.SpanDebug(span, "Body => %s", body)

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {

		p.logger.SpanError(span, "Content-Type=%s, expect application/json", contentType)
		http.Error(w, "invalid Content-Type, expect application/json", http.StatusUnsupportedMediaType)
		return
	}

	var response *AlertmanagerResponse

	data := template.Data{}
	if err := json.Unmarshal(body, &data); err != nil {

		p.logger.SpanError(span, "Can't decode body: %v", err)

		response = &AlertmanagerResponse{
			Message: err.Error(),
		}
	} else {

		channel := strings.TrimLeft(r.URL.Path, "/")
		p.send(span, channel, &data)

		response = &AlertmanagerResponse{
			Message: "OK",
		}
	}

	resp, err := json.Marshal(response)
	if err != nil {
		p.logger.SpanError(span, "Can't encode response: %v", err)
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
	}

	if _, err := w.Write(resp); err != nil {
		p.logger.SpanError(span, "Can't write response: %v", err)
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
	}
}

func NewAlertmanagerProcessor(outputs *common.Outputs, observability *common.Observability) *AlertmanagerProcessor {

	return &AlertmanagerProcessor{
		outputs: outputs,
		tracer:  observability.Traces(),
		logger:  observability.Logs(),
		counter: observability.Metrics().Counter("requests", "Count of all alertmanager processor requests", []string{"status", "channel"}, "alertmanager", "processor"),
	}
}

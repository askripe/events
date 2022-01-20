package processor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/devopsext/events/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/go-playground/webhooks/v6/gitlab"
)

type GitlabProcessor struct {
	outputs *common.Outputs
	tracer  sreCommon.Tracer
	logger  sreCommon.Logger
	counter sreCommon.Counter
	hook    *gitlab.Webhook
}

type GitlabResponse struct {
	Message string
}

func (p *GitlabProcessor) Type() string {
	return "GitlabEvent"
}

func (p *GitlabProcessor) send(span sreCommon.TracerSpan, channel string, o interface{}) {

	e := &common.Event{
		Channel: channel,
		Type:    p.Type(),
		Data:    o,
	}
	if span != nil {
		e.SetSpanContext(span.GetContext())
		e.SetLogger(p.logger)
	}
	p.outputs.Send(e)
	p.counter.Inc(channel)
}

func (p *GitlabProcessor) HandleHttpRequest(w http.ResponseWriter, r *http.Request) {

	span := p.tracer.StartChildSpan(r.Header)
	defer span.Finish()

	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}

	if len(body) == 0 {
		err := errors.New("empty body")
		p.logger.SpanError(span, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	p.logger.SpanDebug(span, "Body => %s", body)

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		p.logger.SpanError(span, "Content-Type=%s, expect application/json", contentType)
		http.Error(w, "invalid Content-Type, expect application/json", http.StatusUnsupportedMediaType)
		return
	}

	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewBuffer(body))

	events := []gitlab.Event{gitlab.PushEvents, gitlab.TagEvents, gitlab.IssuesEvents, gitlab.ConfidentialIssuesEvents, gitlab.CommentEvents,
		gitlab.MergeRequestEvents, gitlab.WikiPageEvents, gitlab.PipelineEvents, gitlab.BuildEvents, gitlab.JobEvents, gitlab.SystemHookEvents}
	payload, err := p.hook.Parse(r, events...)
	if err != nil {
		p.logger.SpanError(span, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	channel := strings.TrimLeft(r.URL.Path, "/")

	switch pl := payload.(type) {
	case gitlab.PushEventPayload:
		p.send(span, channel, payload.(gitlab.PushEventPayload))
	case gitlab.TagEventPayload:
		p.send(span, channel, payload.(gitlab.TagEventPayload))
	case gitlab.IssueEventPayload:
		p.send(span, channel, payload.(gitlab.IssueEventPayload))
	case gitlab.ConfidentialIssueEventPayload:
		p.send(span, channel, payload.(gitlab.ConfidentialIssueEventPayload))
	case gitlab.CommentEventPayload:
		p.send(span, channel, payload.(gitlab.CommentEventPayload))
	case gitlab.MergeRequestEventPayload:
		p.send(span, channel, payload.(gitlab.MergeRequestEventPayload))
	case gitlab.WikiPageEventPayload:
		p.send(span, channel, payload.(gitlab.WikiPageEventPayload))
	case gitlab.PipelineEventPayload:
		p.send(span, channel, payload.(gitlab.PipelineEventPayload))
	case gitlab.BuildEventPayload:
		p.send(span, channel, payload.(gitlab.BuildEventPayload))
	case gitlab.JobEventPayload:
		p.send(span, channel, payload.(gitlab.JobEventPayload))
	case gitlab.SystemHookPayload:
		p.send(span, channel, payload.(gitlab.SystemHookPayload))
	default:
		p.logger.SpanDebug(span, "Not supported %s", pl)
	}

	response := &GitlabResponse{
		Message: "OK",
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

func NewGitlabProcessor(outputs *common.Outputs, logger sreCommon.Logger, tracer sreCommon.Tracer, meter sreCommon.Meter) *GitlabProcessor {

	hook, err := gitlab.New()
	if err != nil {
		logger.Debug("Gitlab processor is disabled.")
		return nil
	}

	return &GitlabProcessor{
		outputs: outputs,
		logger:  logger,
		tracer:  tracer,
		hook:    hook,
		counter: meter.Counter("requests", "Count of all gitlab processor requests", []string{"channel"}, "gitlab", "processor"),
	}
}
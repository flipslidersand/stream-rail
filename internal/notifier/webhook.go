package notifier

import (
	"bytes"
	"fmt"
	"net/http"
	"text/template"
	"time"

	"github.com/flipslidersand/stream-rail/internal/aggregator"
	"github.com/flipslidersand/stream-rail/internal/rule"
)

const defaultWebhookTemplate = `{"rule":"{{.RuleName}}","group":"{{.GroupKey}}","func":"{{.AggFunc}}","value":{{.Value}},"threshold":{{.Threshold}},"op":"{{.Op}}","window_start":"{{.WindowStart.Format "15:04"}}","window_end":"{{.WindowEnd.Format "15:04"}}","corrected":{{.Corrected}}}`

// Webhook sends alert payloads to an HTTP endpoint via text/template rendering.
type Webhook struct {
	url     string
	method  string
	headers map[string]string
	tmpl    *template.Template
	client  *http.Client
}

// NewWebhook builds a Webhook notifier. method defaults to POST when empty.
// headers defaults to Content-Type: application/json when nil. tmplStr is a
// Go text/template; an empty string uses defaultWebhookTemplate.
func NewWebhook(url, method string, headers map[string]string, tmplStr string) (*Webhook, error) {
	if method == "" {
		method = http.MethodPost
	}
	if headers == nil {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	if tmplStr == "" {
		tmplStr = defaultWebhookTemplate
	}
	tmpl, err := template.New("webhook").Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("webhook template: %w", err)
	}
	return &Webhook{
		url:     url,
		method:  method,
		headers: headers,
		tmpl:    tmpl,
		client:  &http.Client{Timeout: 5 * time.Second},
	}, nil
}

// Emit checks the HAVING condition and, if satisfied, POSTs the rendered
// template to the webhook URL. It reports whether an alert fired.
func (w *Webhook) Emit(r rule.Rule, res aggregator.Result) bool {
	if !r.Having.Satisfied(res.Value) {
		return false
	}

	data := newAlertData(r, res)
	var buf bytes.Buffer
	if err := w.tmpl.Execute(&buf, data); err != nil {
		fmt.Printf("[webhook] template error: %v\n", err)
		return true
	}

	req, err := http.NewRequest(w.method, w.url, &buf)
	if err != nil {
		fmt.Printf("[webhook] request error: %v\n", err)
		return true
	}
	for k, v := range w.headers {
		req.Header.Set(k, v)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		fmt.Printf("[webhook] send error: %v\n", err)
		return true
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		fmt.Printf("[webhook] server responded %d for rule=%s\n", resp.StatusCode, r.Name)
	}
	return true
}

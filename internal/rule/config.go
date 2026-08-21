package rule

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// groupByYAML は group_by フィールドを string または []string どちらでも
// 受け付けるカスタム型。単一文字列は要素数 1 のスライスに正規化する。
type groupByYAML []string

func (g *groupByYAML) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		*g = []string{value.Value}
	case yaml.SequenceNode:
		var fields []string
		if err := value.Decode(&fields); err != nil {
			return fmt.Errorf("group_by: %w", err)
		}
		*g = fields
	default:
		return fmt.Errorf("group_by must be a string or list of strings")
	}
	return nil
}

// Config is the fully-parsed representation of rules.yaml.
type Config struct {
	Rules  []Rule
	Notify []NotifyConfig
}

// yamlConfig mirrors the rules.yaml schema in docs/spec.md.
type yamlConfig struct {
	Rules  []yamlRule   `yaml:"rules"`
	Notify []yamlNotify `yaml:"notify"`
}

type yamlNotify struct {
	Type     string            `yaml:"type"`
	URL      string            `yaml:"url"`
	Method   string            `yaml:"method"`
	Headers  map[string]string `yaml:"headers"`
	Template string            `yaml:"template"`
}

type yamlRule struct {
	Name   string `yaml:"name"`
	Source string `yaml:"source"`
	Window struct {
		Type string `yaml:"type"`
		Size string `yaml:"size"`
	} `yaml:"window"`
	Filter struct {
		Field string `yaml:"field"`
		Eq    string `yaml:"eq"`
	} `yaml:"filter"`
	GroupBy   groupByYAML `yaml:"group_by"`
	Aggregate struct {
		Func  string `yaml:"func"`
		Field string `yaml:"field"`
	} `yaml:"aggregate"`
	Having map[string]float64 `yaml:"having"`
	Emit   string             `yaml:"emit"`
}

// LoadConfig reads and validates rules.yaml, returning both the parsed rules
// and the optional notify sink list. Unknown fields and semantic errors fail
// fast so misconfiguration is caught at startup rather than silently ignored.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	var cfg yamlConfig
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Rules) == 0 {
		return Config{}, fmt.Errorf("config %s defines no rules", path)
	}

	rules := make([]Rule, 0, len(cfg.Rules))
	for i, yr := range cfg.Rules {
		r, err := yr.toRule()
		if err != nil {
			return Config{}, fmt.Errorf("rule[%d] %q: %w", i, yr.Name, err)
		}
		rules = append(rules, r)
	}

	notify := make([]NotifyConfig, 0, len(cfg.Notify))
	for i, yn := range cfg.Notify {
		nc, err := yn.toNotifyConfig()
		if err != nil {
			return Config{}, fmt.Errorf("notify[%d]: %w", i, err)
		}
		notify = append(notify, nc)
	}

	return Config{Rules: rules, Notify: notify}, nil
}

// LoadFile reads and validates rules.yaml, returning only the parsed rules.
// It is a convenience wrapper around LoadConfig for callers that do not need
// the notify sink configuration.
func LoadFile(path string) ([]Rule, error) {
	cfg, err := LoadConfig(path)
	if err != nil {
		return nil, err
	}
	return cfg.Rules, nil
}

func (yr yamlRule) toRule() (Rule, error) {
	if yr.Name == "" {
		return Rule{}, fmt.Errorf("name is required")
	}

	// Window: only tumbling is supported today (see ADR-002).
	if yr.Window.Type != "" && yr.Window.Type != "tumbling" {
		return Rule{}, fmt.Errorf("unsupported window.type %q (only tumbling)", yr.Window.Type)
	}
	var size time.Duration
	if yr.Window.Size != "" {
		d, err := time.ParseDuration(yr.Window.Size)
		if err != nil {
			return Rule{}, fmt.Errorf("invalid window.size %q: %w", yr.Window.Size, err)
		}
		if d <= 0 {
			return Rule{}, fmt.Errorf("window.size must be positive, got %s", yr.Window.Size)
		}
		size = d
	}

	// Grouping: any field is allowed (service/level or a Fields key). The engine
	// derives a GroupFunc from group_by (#17); an empty value defaults to service.

	// Aggregate.
	switch yr.Aggregate.Func {
	case AggCount:
	case AggSum, AggMax, AggMin, AggAvg:
		if yr.Aggregate.Field == "" {
			return Rule{}, fmt.Errorf("aggregate.field is required for %s", yr.Aggregate.Func)
		}
	default:
		return Rule{}, fmt.Errorf("unsupported aggregate.func %q (count|sum|max|min|avg)", yr.Aggregate.Func)
	}

	// Having: exactly one operator key.
	having, err := parseHaving(yr.Having)
	if err != nil {
		return Rule{}, err
	}

	// Emit.
	if yr.Emit != "" && yr.Emit != "console" {
		return Rule{}, fmt.Errorf("unsupported emit %q (only console currently)", yr.Emit)
	}

	return Rule{
		Name:       yr.Name,
		Filter:     Filter{Field: yr.Filter.Field, Eq: yr.Filter.Eq},
		GroupBy:    []string(yr.GroupBy),
		AggFunc:    yr.Aggregate.Func,
		AggField:   yr.Aggregate.Field,
		Having:     having,
		Emit:       yr.Emit,
		WindowSize: size,
	}, nil
}

func (yn yamlNotify) toNotifyConfig() (NotifyConfig, error) {
	switch yn.Type {
	case "console":
	case "webhook":
		if yn.URL == "" {
			return NotifyConfig{}, fmt.Errorf("webhook notify requires url")
		}
	default:
		return NotifyConfig{}, fmt.Errorf("unsupported notify type %q (console|webhook)", yn.Type)
	}
	return NotifyConfig{
		Type:     yn.Type,
		URL:      yn.URL,
		Method:   yn.Method,
		Headers:  yn.Headers,
		Template: yn.Template,
	}, nil
}

func parseHaving(m map[string]float64) (Having, error) {
	if len(m) != 1 {
		return Having{}, fmt.Errorf("having must have exactly one operator (gt|gte|lt|lte), got %d", len(m))
	}
	for op, v := range m {
		switch op {
		case OpGT, OpGTE, OpLT, OpLTE:
			return Having{Op: op, Value: v}, nil
		default:
			return Having{}, fmt.Errorf("unsupported having operator %q", op)
		}
	}
	return Having{}, fmt.Errorf("having is empty")
}

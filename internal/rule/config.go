package rule

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// yamlConfig mirrors the rules.yaml schema in docs/spec.md.
type yamlConfig struct {
	Rules []yamlRule `yaml:"rules"`
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
	GroupBy   string `yaml:"group_by"`
	Aggregate struct {
		Func  string `yaml:"func"`
		Field string `yaml:"field"`
	} `yaml:"aggregate"`
	Having map[string]float64 `yaml:"having"`
	Emit   string             `yaml:"emit"`
}

// LoadFile reads and validates rules.yaml, returning the parsed rules.
// Unknown fields and semantic errors fail fast so misconfiguration is caught
// at startup rather than silently ignored.
func LoadFile(path string) ([]Rule, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	var cfg yamlConfig
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("config %s defines no rules", path)
	}

	rules := make([]Rule, 0, len(cfg.Rules))
	for i, yr := range cfg.Rules {
		r, err := yr.toRule()
		if err != nil {
			return nil, fmt.Errorf("rule[%d] %q: %w", i, yr.Name, err)
		}
		rules = append(rules, r)
	}
	return rules, nil
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
	case AggSum:
		if yr.Aggregate.Field == "" {
			return Rule{}, fmt.Errorf("aggregate.field is required for sum")
		}
	default:
		return Rule{}, fmt.Errorf("unsupported aggregate.func %q (count|sum)", yr.Aggregate.Func)
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
		GroupBy:    yr.GroupBy,
		AggFunc:    yr.Aggregate.Func,
		AggField:   yr.Aggregate.Field,
		Having:     having,
		Emit:       yr.Emit,
		WindowSize: size,
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

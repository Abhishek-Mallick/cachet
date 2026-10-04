package ctl

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/Abhishek-Mallick/cachet/internal/config"
)

// Checking that a rendered Helm chart produces a config Cachet accepts.
//
// It exists because the chart rendered perfectly for months while emitting a shard shape the loader
// does not read and naming Secret variables it does not consume. `helm lint` and `helm template`
// were both satisfied: they ask whether a chart RENDERS, and nobody was asking whether what came
// out would boot.
//
// A command rather than a script, so the check runs against the same loader the engine runs, and so
// an operator who has customised the chart can run it on their own values before a deploy.

// CheckRendered extracts the ConfigMap from a rendered chart and loads it.
//
// The environment the Deployment would supply is passed in: the chart's DSNs name variables, and
// checking a config that cannot expand them would fail for the wrong reason.
func CheckRendered(path string, env map[string]string) (RenderedReport, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own argument
	if err != nil {
		return RenderedReport{}, fmt.Errorf("ctl: read %s: %w", path, err)
	}

	body, err := configMapFrom(string(raw))
	if err != nil {
		return RenderedReport{}, err
	}

	tmp, err := os.CreateTemp("", "cachet-rendered-*.yaml")
	if err != nil {
		return RenderedReport{}, fmt.Errorf("ctl: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.WriteString(body); err != nil {
		_ = tmp.Close()
		return RenderedReport{}, fmt.Errorf("ctl: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return RenderedReport{}, fmt.Errorf("ctl: %w", err)
	}

	cfg, err := config.Load(tmp.Name(), env)
	if err != nil {
		return RenderedReport{}, fmt.Errorf("the config this chart renders does not load: %w\n\n%s", err, body)
	}

	report := RenderedReport{
		Shards:     len(cfg.Shards),
		Topologies: len(cfg.Topologies),
	}
	for _, t := range cfg.Tables {
		report.Tables = append(report.Tables, t.Name)
	}
	return report, nil
}

// RenderedReport is what a rendered chart turned out to declare.
type RenderedReport struct {
	Shards     int      `json:"shards"`
	Topologies int      `json:"topologies"`
	Tables     []string `json:"tables"`
}

// String renders the report for a human.
func (r RenderedReport) String() string {
	return fmt.Sprintf("the rendered config loads: %d shard(s), %d topology(ies), tables %v\n",
		r.Shards, r.Topologies, r.Tables)
}

// configMapFrom finds the config.yaml inside a rendered chart's documents.
//
// A rendered chart is a stream of Kubernetes objects; exactly one of them carries the file the
// engine reads. Parsing the stream rather than pattern-matching the text, so a Deployment that
// happens to mention `config.yaml` cannot be mistaken for it.
func configMapFrom(rendered string) (string, error) {
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var doc struct {
			Kind string            `yaml:"kind"`
			Data map[string]string `yaml:"data"`
		}
		err := dec.Decode(&doc)
		if err != nil {
			break
		}
		if doc.Kind != "ConfigMap" {
			continue
		}
		if body, ok := doc.Data["config.yaml"]; ok {
			return body, nil
		}
	}
	return "", fmt.Errorf("ctl: no ConfigMap with a config.yaml in the rendered chart")
}

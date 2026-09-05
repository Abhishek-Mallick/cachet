package harness

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Distribution names a key-selection strategy.
type Distribution string

// The supported distributions. Each backs a workload in the catalogue (benchmarking doc §5).
const (
	// DistUniform backs W1, the control. Never a headline number.
	DistUniform Distribution = "uniform"
	// DistZipfian backs W2, the primary workload every phase row uses.
	DistZipfian Distribution = "zipfian"
	// DistHot backs W3, the stampede: every request on one key.
	DistHot Distribution = "hot"
)

// Workload is one entry in the benchmark catalogue.
type Workload struct {
	ID           string        `yaml:"id"`
	Name         string        `yaml:"name"`
	Distribution Distribution  `yaml:"distribution"`
	Theta        float64       `yaml:"theta"`
	ReadFraction float64       `yaml:"read_fraction"`
	Rows         uint64        `yaml:"rows"`
	Rate         int           `yaml:"rate"`
	Workers      int           `yaml:"workers"`
	Warmup       time.Duration `yaml:"warmup"`
	Measure      time.Duration `yaml:"measure"`
	Seed         int64         `yaml:"seed"`
	HotKey       uint64        `yaml:"hot_key"`
}

// LoadWorkload reads and validates a workload definition.
func LoadWorkload(path string) (Workload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Workload{}, fmt.Errorf("harness: read %s: %w", path, err)
	}

	var w Workload
	if err := yaml.Unmarshal(data, &w); err != nil {
		return Workload{}, fmt.Errorf("harness: parse %s: %w", path, err)
	}
	if err := w.validate(); err != nil {
		return Workload{}, fmt.Errorf("harness: %s: %w", path, err)
	}
	return w, nil
}

func (w Workload) validate() error {
	if strings.TrimSpace(w.ID) == "" {
		return fmt.Errorf("workload has no id")
	}
	if w.Rows == 0 {
		return fmt.Errorf("workload %s has no rows", w.ID)
	}
	if w.Rate <= 0 {
		return fmt.Errorf("workload %s has no rate", w.ID)
	}
	if w.Workers <= 0 {
		return fmt.Errorf("workload %s has no workers", w.ID)
	}
	if w.Measure <= 0 {
		return fmt.Errorf("workload %s has no measure window", w.ID)
	}
	if w.ReadFraction < 0 || w.ReadFraction > 1 {
		return fmt.Errorf("workload %s has read_fraction %v, want [0,1]", w.ID, w.ReadFraction)
	}

	switch w.Distribution {
	case DistZipfian:
		// θ is what makes W2 the primary workload rather than a second control. Defaulting it
		// silently would let a run claim to be skewed while producing flat traffic.
		if w.Theta <= 0 {
			return fmt.Errorf("workload %s is zipfian but has no theta", w.ID)
		}
	case DistUniform, DistHot:
		// Accepting and ignoring θ would let a control workload look configured for skew.
		if w.Theta != 0 {
			return fmt.Errorf("workload %s is %s, so theta is meaningless", w.ID, w.Distribution)
		}
	default:
		return fmt.Errorf("workload %s has unknown distribution %q", w.ID, w.Distribution)
	}
	return nil
}

// Generator builds the key generator this workload describes.
func (w Workload) Generator() (Generator, error) {
	switch w.Distribution {
	case DistUniform:
		return NewUniform(w.Rows, w.Seed), nil
	case DistZipfian:
		return NewZipfian(w.Rows, w.Theta, w.Seed)
	case DistHot:
		return NewHot(w.HotKey), nil
	default:
		return nil, fmt.Errorf("harness: unknown distribution %q", w.Distribution)
	}
}

// Params renders the workload as the parameter block recorded in a results file.
func (w Workload) Params() Params {
	return Params{
		WarmupSeconds:  int(w.Warmup.Seconds()),
		MeasureSeconds: int(w.Measure.Seconds()),
		Rate:           w.Rate,
		Workers:        w.Workers,
		Seed:           w.Seed,
		ZipfTheta:      w.Theta,
		Rows:           w.Rows,
		ReadFraction:   w.ReadFraction,
	}
}

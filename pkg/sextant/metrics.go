package sextant

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// What Sextant publishes, and the label that keeps it honest.
//
// Every series carries the TIER. A verifier that could only compare values cannot evaluate SESSION
// or BOUNDED, and a dashboard showing "cachet_sextant_consistency_nines" with no tier on it would
// let a value-tier run be read as a version-tier claim. That is the one failure that would make
// this component worse than not existing, because it arrives with a reassuring number attached.
//
// Levels the verifier cannot evaluate are not exported at all. Exporting them as zero violations
// would be a clean bill of health nothing measured.

// RegisterMetrics exports a verifier's SLO on a Prometheus registry.
//
// In this package rather than in the binary, because a verifier somebody points at their own Redis
// has to publish the same series with the same meaning — otherwise the number cannot be compared
// with anybody else's, and a consistency figure that is not comparable is decoration.
func RegisterMetrics(reg prometheus.Registerer, slo *SLO, v *Verifier) error {
	// Tier and key source both travel on every series. They answer two different questions an
	// operator has to be able to ask of a number: what comparison produced it, and over which
	// population.
	common := prometheus.Labels{"tier": string(v.Tier()), "keys": string(v.KeySourceKind())}

	if err := reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "cachet", Subsystem: "sextant", Name: "shadow_mode",
		Help:        "1 when observing a deployment that serves no application traffic.",
		ConstLabels: common,
	}, func() float64 {
		if v.Shadow() {
			return 1
		}
		return 0
	})); err != nil {
		return err
	}

	for _, level := range v.Levels() {
		l := level
		labels := prometheus.Labels{
			"level": l.String(), "tier": string(v.Tier()), "keys": string(v.KeySourceKind()),
		}

		// Nines are gauged rather than the raw fraction, because that is how this gets discussed —
		// and because a fraction rounded for display hides the difference between 0.999 and 0.99999
		// exactly where it matters most.
		gauges := []struct {
			name, help string
			value      func() float64
		}{
			{
				"consistency_nines", "Measured consistency per level, in nines. Absent until observations exist.",
				func() float64 { return slo.Report(l, time.Now()).Nines },
			},
			{
				"observations", "Observations in the current window. Zero means the figure above is not evidence.",
				func() float64 { return float64(slo.Report(l, time.Now()).Observations) },
			},
			{
				"violations", "Violations in the current window.",
				func() float64 { return float64(slo.Report(l, time.Now()).Violations) },
			},
		}
		for _, g := range gauges {
			if err := reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: "cachet", Subsystem: "sextant", Name: g.name,
				Help: g.help, ConstLabels: labels,
			}, g.value)); err != nil {
				return err
			}
		}
	}
	return nil
}

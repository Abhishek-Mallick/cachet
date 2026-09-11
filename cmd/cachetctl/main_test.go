package main

import (
	"flag"
	"strings"
	"testing"
)

func TestFlagsAfterThePositionalArgumentAreParsed(t *testing.T) {
	t.Parallel()

	// Go's flag package stops parsing at the first non-flag argument, so `inspect <key> -config x`
	// would silently treat -config and its value as two more positional arguments. Every operator
	// writes the subject of the command first; a tool that only accepts `-config x <key>` fails in
	// the most natural usage and reports it as a usage error about the key.
	for _, tc := range []struct {
		name      string
		args      []string
		wantFlags []string
		wantPos   []string
	}{
		{
			name:      "flag after positional",
			args:      []string{"entities:5000", "-config", "/tmp/c.yaml"},
			wantFlags: []string{"-config", "/tmp/c.yaml"},
			wantPos:   []string{"entities:5000"},
		},
		{
			name:      "flag before positional still works",
			args:      []string{"-config", "/tmp/c.yaml", "entities:5000"},
			wantFlags: []string{"-config", "/tmp/c.yaml"},
			wantPos:   []string{"entities:5000"},
		},
		{
			name:      "boolean flag after positional",
			args:      []string{"entities:5000", "-json"},
			wantFlags: []string{"-json"},
			wantPos:   []string{"entities:5000"},
		},
		{
			name:      "equals form after positional",
			args:      []string{"entities:5000", "-config=/tmp/c.yaml"},
			wantFlags: []string{"-config=/tmp/c.yaml"},
			wantPos:   []string{"entities:5000"},
		},
		{
			name:      "double dash form",
			args:      []string{"entities:5000", "--json"},
			wantFlags: []string{"--json"},
			wantPos:   []string{"entities:5000"},
		},
		{
			name:      "no positional",
			args:      []string{"-config", "/tmp/c.yaml", "-json"},
			wantFlags: []string{"-config", "/tmp/c.yaml", "-json"},
			wantPos:   nil,
		},
		{
			name:      "nothing at all",
			args:      nil,
			wantFlags: nil,
			wantPos:   nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(nopWriter{})
			_ = fs.String("config", "", "")
			_ = fs.Bool("json", false, "")

			flags, pos := permute(fs, tc.args)
			if strings.Join(flags, " ") != strings.Join(tc.wantFlags, " ") {
				t.Errorf("flags = %v, want %v", flags, tc.wantFlags)
			}
			if strings.Join(pos, " ") != strings.Join(tc.wantPos, " ") {
				t.Errorf("positionals = %v, want %v", pos, tc.wantPos)
			}
		})
	}
}

func TestParseArgsExposesBothFlagsAndPositionals(t *testing.T) {
	t.Parallel()

	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(nopWriter{})
	configPath := fs.String("config", "", "")
	asJSON := fs.Bool("json", false, "")

	rest, err := parseArgs(fs, []string{"entities:5000", "-config", "/tmp/c.yaml", "-json"})
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}

	if len(rest) != 1 || rest[0] != "entities:5000" {
		t.Errorf("positional args = %v, want [entities:5000]", rest)
	}
	if *configPath != "/tmp/c.yaml" {
		t.Errorf("-config = %q, want /tmp/c.yaml", *configPath)
	}
	if !*asJSON {
		t.Error("-json was not set")
	}
}

func TestParseArgsRejectsAnUnknownFlag(t *testing.T) {
	t.Parallel()

	// A mistyped flag must be an error, never silently reinterpreted as the key to act on. This
	// tool has a manual invalidation in it, and `-drynrun` becoming a positional argument would be
	// a typo that fires the real thing.
	fs := flag.NewFlagSet("invalidate", flag.ContinueOnError)
	fs.SetOutput(nopWriter{})
	_ = fs.Bool("dry-run", false, "")

	if _, err := parseArgs(fs, []string{"entities:1", "-drynrun"}); err == nil {
		t.Error("parseArgs accepted an unknown flag instead of rejecting it")
	}
}

func TestUnknownCommandIsAnError(t *testing.T) {
	t.Parallel()

	if err := run([]string{"nonsense"}); err == nil {
		t.Error("run accepted an unknown command")
	}
}

func TestNoCommandIsAnError(t *testing.T) {
	t.Parallel()

	if err := run(nil); err == nil {
		t.Error("run accepted no command at all")
	}
}

func TestHelpAndVersionSucceed(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{"help", "--help", "-h", "version", "--version", "-version"} {
		if err := run([]string{arg}); err != nil {
			t.Errorf("run(%q) returned %v, want success", arg, err)
		}
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

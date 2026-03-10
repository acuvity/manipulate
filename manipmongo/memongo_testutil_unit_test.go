package manipmongo

import (
	"testing"
	"time"
)

func TestRequireMemongoStrictModeAliases(t *testing.T) {
	t.Setenv(requireMemongoEnv, "")
	t.Setenv(requireMongoEnv, "")
	if requireMemongoStrictMode() {
		t.Fatalf("expected strict mode to be disabled when both env vars are unset")
	}

	t.Setenv(requireMemongoEnv, "true")
	if !requireMemongoStrictMode() {
		t.Fatalf("expected strict mode enabled by %s", requireMemongoEnv)
	}

	t.Setenv(requireMemongoEnv, "")
	t.Setenv(requireMongoEnv, "yes")
	if !requireMemongoStrictMode() {
		t.Fatalf("expected strict mode enabled by %s", requireMongoEnv)
	}
}

func TestParseDockerMappedPort(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "ipv4", input: "0.0.0.0:49153\n", want: "49153"},
		{name: "ipv6", input: ":::49154\n", want: "49154"},
		{name: "bracketed ipv6", input: "[::]:49155\n0.0.0.0:49156\n", want: "49155"},
		{name: "invalid", input: "garbage\n", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDockerMappedPort(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got port=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected port: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestParseDockerRunContainerID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "single line id",
			input: "9d63ee5b5b7dd66bc94631b429a74a4a6f5c28cf156506f2de03dea91f9b377e\n",
			want:  "9d63ee5b5b7dd66bc94631b429a74a4a6f5c28cf156506f2de03dea91f9b377e",
		},
		{
			name:  "output with pull progress",
			input: "Unable to find image 'mongo:latest' locally\nlatest: Pulling from library/mongo\nstatus line\n9d63ee5b5b7dd66bc94631b429a74a4a6f5c28cf156506f2de03dea91f9b377e\n",
			want:  "9d63ee5b5b7dd66bc94631b429a74a4a6f5c28cf156506f2de03dea91f9b377e",
		},
		{
			name:    "invalid",
			input:   "no container id here\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDockerRunContainerID(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got id=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected id: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestMongoDockerConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv(mongoTestDockerImageEnv, "")
	t.Setenv(mongoTestDockerStartupTimeoutEnv, "")
	if got := mongoTestDockerImage(); got != defaultMongoTestDockerImage {
		t.Fatalf("unexpected default image: got %q want %q", got, defaultMongoTestDockerImage)
	}
	if got := mongoTestDockerStartupTimeout(); got != defaultMongoTestDockerStartupTimeout {
		t.Fatalf("unexpected default startup timeout: got %s want %s", got, defaultMongoTestDockerStartupTimeout)
	}

	t.Setenv(mongoTestDockerImageEnv, "mongo:7")
	t.Setenv(mongoTestDockerStartupTimeoutEnv, "90s")
	if got := mongoTestDockerImage(); got != "mongo:7" {
		t.Fatalf("unexpected override image: %q", got)
	}
	if got := mongoTestDockerStartupTimeout(); got != 90*time.Second {
		t.Fatalf("unexpected override startup timeout: got %s want 90s", got)
	}

	t.Setenv(mongoTestDockerStartupTimeoutEnv, "bad-duration")
	if got := mongoTestDockerStartupTimeout(); got != defaultMongoTestDockerStartupTimeout {
		t.Fatalf("expected invalid duration to fall back to default: got %s want %s", got, defaultMongoTestDockerStartupTimeout)
	}
}

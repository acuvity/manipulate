package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var (
	mongoEndpointOnce    sync.Once
	mongoEndpointURI     string
	mongoEndpointErr     error
	mongoEndpointCleanup func()
)

const requireMemongoEnv = "REQUIRE_MEMONGO"
const requireMongoEnv = "REQUIRE_MONGO"
const mongoTestURIEnv = "MONGO_TEST_URI"
const mongoTestDockerImageEnv = "MONGO_TEST_DOCKER_IMAGE"
const mongoTestDockerStartupTimeoutEnv = "MONGO_TEST_DOCKER_STARTUP_TIMEOUT"
const mongoTestDockerDisableEnv = "MONGO_TEST_DOCKER_DISABLE"

// Default to a stable server image for integration tests.
const defaultMongoTestDockerImage = "mongo:4.4.26"
const defaultMongoTestDockerStartupTimeout = 60 * time.Second

var dockerCommand = func(args ...string) ([]byte, error) {
	cmd := exec.Command("docker", args...)
	return cmd.CombinedOutput()
}

func envBoolTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func requireMemongoStrictMode() bool {
	return envBoolTrue(os.Getenv(requireMemongoEnv)) || envBoolTrue(os.Getenv(requireMongoEnv))
}

func skipOrFailMemongo(t *testing.T, format string, args ...any) {
	t.Helper()
	if requireMemongoStrictMode() {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

func requireMemongo(t *testing.T) (uri string, db string) {
	t.Helper()

	mongoEndpointOnce.Do(func() {
		mongoEndpointURI, mongoEndpointCleanup, mongoEndpointErr = resolveMongoTestEndpoint()
	})

	if mongoEndpointErr != nil {
		skipOrFailMemongo(t, "skipping MongoDB integration test: unable to provision mongo test endpoint: %v", mongoEndpointErr)
	}

	db = dbNameForTest(t)

	t.Cleanup(func() {
		_ = DropDatabaseByURI(mongoEndpointURI, db)
	})

	return mongoEndpointURI, db
}

func resolveMongoTestEndpoint() (string, func(), error) {
	var errs []error

	if uri := strings.TrimSpace(os.Getenv(mongoTestURIEnv)); uri != "" {
		if err := pingMongoURI(uri, 5*time.Second); err == nil {
			return uri, nil, nil
		} else {
			errs = append(errs, fmt.Errorf("%s is set but unavailable: %w", mongoTestURIEnv, err))
		}
	}

	if envBoolTrue(os.Getenv(mongoTestDockerDisableEnv)) {
		errs = append(errs, fmt.Errorf("docker provisioning disabled by %s", mongoTestDockerDisableEnv))
		return "", nil, errors.Join(errs...)
	}

	uri, cleanup, err := startDockerMongo()
	if err == nil {
		return uri, cleanup, nil
	}

	errs = append(errs, fmt.Errorf("docker mongo provisioning failed: %w", err))
	return "", nil, errors.Join(errs...)
}

func mongoTestDockerImage() string {
	if image := strings.TrimSpace(os.Getenv(mongoTestDockerImageEnv)); image != "" {
		return image
	}
	return defaultMongoTestDockerImage
}

func mongoTestDockerStartupTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv(mongoTestDockerStartupTimeoutEnv)); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultMongoTestDockerStartupTimeout
}

func startDockerMongo() (uri string, cleanup func(), err error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return "", nil, fmt.Errorf("docker executable not found: %w", err)
	}
	if out, err := dockerCommand("info", "--format", "{{.ServerVersion}}"); err != nil {
		return "", nil, fmt.Errorf("docker daemon unavailable: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	containerName := fmt.Sprintf("manipmongo-test-%d", time.Now().UnixNano())
	out, err := dockerCommand("run", "--rm", "-d", "-P", "--name", containerName, mongoTestDockerImage(), "--bind_ip_all")
	if err != nil {
		return "", nil, fmt.Errorf("docker run failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	containerID, err := parseDockerRunContainerID(string(out))
	if err != nil {
		return "", nil, fmt.Errorf("unable to parse docker run container id from output %q: %w", strings.TrimSpace(string(out)), err)
	}

	cleanup = func() {
		_, _ = dockerCommand("rm", "-f", containerID)
	}

	mappedPort, err := dockerMappedMongoPort(containerID)
	if err != nil {
		cleanup()
		return "", nil, err
	}

	uri = fmt.Sprintf("mongodb://127.0.0.1:%s", mappedPort)
	if err := waitForMongoReady(uri, mongoTestDockerStartupTimeout()); err != nil {
		cleanup()
		return "", nil, err
	}

	return uri, cleanup, nil
}

func parseDockerRunContainerID(output string) (string, error) {
	lines := strings.Split(output, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if isHexString(line) && len(line) >= 12 {
			return line, nil
		}
	}
	return "", errors.New("container id not found")
}

func isHexString(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return s != ""
}

func dockerMappedMongoPort(containerID string) (string, error) {
	out, err := dockerCommand("port", containerID, "27017/tcp")
	if err != nil {
		return "", fmt.Errorf("docker port failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}

	port, err := parseDockerMappedPort(string(out))
	if err != nil {
		return "", fmt.Errorf("unable to parse mapped mongo port from docker output %q: %w", strings.TrimSpace(string(out)), err)
	}

	return port, nil
}

func parseDockerMappedPort(output string) (string, error) {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		idx := strings.LastIndex(line, ":")
		if idx == -1 || idx == len(line)-1 {
			continue
		}

		port := strings.TrimSpace(line[idx+1:])
		if _, err := strconv.Atoi(port); err == nil {
			return port, nil
		}
	}

	return "", errors.New("port not found")
}

func waitForMongoReady(uri string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		if err := pingMongoURI(uri, 3*time.Second); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(300 * time.Millisecond)
	}

	if lastErr == nil {
		lastErr = errors.New("timeout while waiting for ping response")
	}

	return fmt.Errorf("mongo at %s not ready within %s: %w", uri, timeout, lastErr)
}

func pingMongoURI(uri string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(
		mongooptions.Client().
			ApplyURI(uri).
			SetConnectTimeout(timeout).
			SetTimeout(timeout),
	)
	if err != nil {
		return err
	}

	defer func() {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), timeout)
		defer disconnectCancel()
		_ = client.Disconnect(disconnectCtx)
	}()

	return client.Ping(ctx, readpref.Primary())
}

func dbNameForTest(t *testing.T) string {
	name := sanitizeName(t.Name())
	if len(name) > 8 {
		name = name[:8]
	}
	return fmt.Sprintf("mm_%s_%d", name, time.Now().UnixNano())
}

func sanitizeName(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		case r >= '0' && r <= '9':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// DropDatabaseByURI is test-only cleanup helper.
func DropDatabaseByURI(uri, db string) error {
	m, err := New(uri, db)
	if err != nil {
		return err
	}
	return DropDatabase(m)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if mongoEndpointCleanup != nil {
		mongoEndpointCleanup()
	}
	os.Exit(code)
}

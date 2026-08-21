package execution

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	defaultLeaseDuration     = 30 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
)

type engineConfig struct {
	clock             Clock
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	workerNamespace   string
}

// EngineOption customizes reliability timing or worker identity.
type EngineOption func(*engineConfig) error

// WithClock supplies the engine's scheduler clock.
func WithClock(clock Clock) EngineOption {
	return func(config *engineConfig) error {
		if clock == nil {
			return errors.New("clock must not be nil")
		}
		config.clock = clock
		return nil
	}
}

// WithLeaseTiming configures task lease duration and worker heartbeat cadence.
// The heartbeat interval must be shorter than the lease duration.
func WithLeaseTiming(leaseDuration, heartbeatInterval time.Duration) EngineOption {
	return func(config *engineConfig) error {
		config.leaseDuration = leaseDuration
		config.heartbeatInterval = heartbeatInterval
		return nil
	}
}

// WithWorkerNamespace supplies a stable namespace for deterministic tests or a
// caller-managed worker process identity. Production callers should normally
// use the random per-engine default.
func WithWorkerNamespace(namespace string) EngineOption {
	return func(config *engineConfig) error {
		if !validIdentifier(namespace) {
			return fmt.Errorf("worker namespace %q is invalid", namespace)
		}
		config.workerNamespace = namespace
		return nil
	}
}

func defaultEngineConfig() (engineConfig, error) {
	namespaceBytes := make([]byte, 8)
	if _, err := rand.Read(namespaceBytes); err != nil {
		return engineConfig{}, fmt.Errorf("generate worker namespace: %w", err)
	}
	return engineConfig{
		clock:             systemClock{},
		leaseDuration:     defaultLeaseDuration,
		heartbeatInterval: defaultHeartbeatInterval,
		workerNamespace:   "local-" + hex.EncodeToString(namespaceBytes),
	}, nil
}

func (config engineConfig) validate() error {
	if config.clock == nil {
		return errors.New("clock must not be nil")
	}
	if config.leaseDuration <= 0 {
		return errors.New("lease duration must be positive")
	}
	if config.heartbeatInterval <= 0 {
		return errors.New("heartbeat interval must be positive")
	}
	if config.heartbeatInterval >= config.leaseDuration {
		return errors.New("heartbeat interval must be shorter than lease duration")
	}
	if !validIdentifier(config.workerNamespace) {
		return errors.New("worker namespace is invalid")
	}
	return nil
}

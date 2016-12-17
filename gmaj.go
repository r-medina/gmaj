package gmaj

import (
	"errors"
	"time"

	"google.golang.org/grpc"
)

const dfltkeyLen = 8

// the configuration for whole package
var cfg Config

// configuration errors
var (
	ErrBadKeyLen = errors.New("key length must be <= 128 and divisible by 8")
	ErrBadIDLen  = errors.New("ID length must be  key length/8")
)

// DefaultConfig is the default configuration.
var DefaultConfig = &Config{
	KeyLength:             dfltkeyLen,
	IDLength:              dfltkeyLen / 8, // key length bytes
	FixNextFingerInterval: 25 * time.Millisecond,
	StabilizeInterval:     75 * time.Millisecond,
	RetryInterval:         150 * time.Millisecond,
	DialOptions: []grpc.DialOption{
		grpc.WithInsecure(), // TODO(ricky): find a better way to use this for testing
		grpc.WithTimeout(time.Second),
	},
}

func init() {
	SetConfig(DefaultConfig)
}

// Config contains all the configuration information for a gmaj node.
type Config struct {
	KeyLength             int // the number of bits (i.e. M value),  <= 128 and divisible by 8
	IDLength              int // must be KeyLength/8
	FixNextFingerInterval time.Duration
	StabilizeInterval     time.Duration
	ConnectionTimeout     time.Duration
	RetryInterval         time.Duration
	DialOptions           []grpc.DialOption
}

// Validate checks some of the values of a Config to make sure they are valid.
func (config *Config) Validate() error {
	if (config.KeyLength > 128) || (config.KeyLength%8 != 0) {
		return ErrBadKeyLen
	}

	if config.IDLength != config.KeyLength/8 {
		return ErrBadIDLen
	}

	return nil
}

// SetConfig sets the configuration for the whole package.
func SetConfig(config *Config) error {
	if err := config.Validate(); err != nil {
		return err
	}

	cfg = *config

	return nil
}

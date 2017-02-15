package gmajcfg

import (
	"errors"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

const dfltKeySize = 8

// configuration errors
var (
	ErrBadKeyLen = errors.New("key length must be divisible by 8")
	ErrBadIDLen  = errors.New("ID length must be  key length/8")
)

// Config contains all the configuration information for a gmaj node.
type Config struct {
	// KeySize is the number of bits (i.e. M value), divisible by 8
	KeySize               int
	IDLength              int // must be KeyLength/8
	FixNextFingerInterval time.Duration
	StabilizeInterval     time.Duration
	ConnectionTimeout     time.Duration
	RetryInterval         time.Duration
	DialOptions           []grpc.DialOption

	Log grpclog.Logger
}

// Validate checks some of the values of a Config to make sure they are valid.
func (config *Config) Validate() error {
	if config.KeySize%8 != 0 {
		return ErrBadKeyLen
	}

	if config.IDLength != config.KeySize/8 {
		return ErrBadIDLen
	}

	return nil
}

// DefaultConfig is the default configuration.
var DefaultConfig = &Config{
	KeySize:               dfltKeySize,
	IDLength:              dfltKeySize / 8, // key length bytes
	FixNextFingerInterval: 25 * time.Millisecond,
	StabilizeInterval:     50 * time.Millisecond,
	RetryInterval:         75 * time.Millisecond,
	DialOptions: []grpc.DialOption{
		grpc.WithInsecure(), // TODO(ricky): find a better way to use this for testing
		grpc.WithTimeout(time.Second),
	},
	Log: log.New(os.Stderr, "gmaj: ", log.LstdFlags),
}

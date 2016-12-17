package gmaj

import (
	"errors"
	"math/big"
	"time"

	"google.golang.org/grpc"
)

const dfltKeySize = 8

// the configuration for whole package
var cfg Config

// configuration errors
var (
	ErrBadKeyLen = errors.New("key length must be <= 128 and divisible by 8")
	ErrBadIDLen  = errors.New("ID length must be  key length/8")
)

// DefaultConfig is the default configuration.
var DefaultConfig = &Config{
	KeySize:               dfltKeySize,
	IDLength:              dfltKeySize / 8, // key length bytes
	FixNextFingerInterval: 25 * time.Millisecond,
	StabilizeInterval:     75 * time.Millisecond,
	RetryInterval:         150 * time.Millisecond,
	DialOptions: []grpc.DialOption{
		grpc.WithInsecure(), // TODO(ricky): find a better way to use this for testing
		grpc.WithTimeout(time.Second),
	},
}

// maximum ID value
var max *big.Int

func init() {
	SetConfig(DefaultConfig)

	max = func() *big.Int {
		max := big.NewInt(2)

		b2 := big.NewInt(2)
		for i := 0; i < cfg.KeySize; i++ {
			max.Mul(max, b2)
		}

		return max
	}()
}

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

// SetConfig sets the configuration for the whole package.
func SetConfig(config *Config) error {
	if err := config.Validate(); err != nil {
		return err
	}

	cfg = *config

	return nil
}

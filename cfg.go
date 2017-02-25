package gmaj

import (
	"errors"
	"math/big"
	"sync"

	"github.com/r-medina/gmaj/gmajcfg"
)

var errSetConfig = errors.New("cannot set configuration more than once")

// the configuration for whole package
var config struct {
	gmajcfg.Config
	// the largest possible ID value
	max *big.Int
	o   sync.Once
}

func init() {
	mustInit(gmajcfg.DefaultConfig)
}

// Init allows consumers of this package to set the configuration. This has to
// happen before any other functionality of the package is used.
// Should only be called once.
func Init(cfg *gmajcfg.Config) error {
	err := errSetConfig
	config.o.Do(func() {
		if err = cfg.Validate(); err != nil {
			return
		}

		config.Config = *cfg
		config.max = getMax()
	})

	return err
}

func mustInit(cfg *gmajcfg.Config) {
	if err := cfg.Validate(); err != nil {
		config.Log.Fatalf("error setting configuration: %v", err)
	}

	config.Config = *cfg
	config.max = getMax()
}

func getMax() *big.Int {
	max := big.NewInt(2)

	b2 := big.NewInt(2)
	for i := 0; i < config.KeySize; i++ {
		max.Mul(max, b2)
	}

	return max
}

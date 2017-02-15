package gmaj

import (
	"errors"
	"math/big"
	"sync"

	"github.com/r-medina/gmaj/gmajcfg"
)

var errSetConfig = errors.New("cannot set configuration more than once")

// the configuration for whole package
var config gmajcfg.Config

// the largest possible ID value
var max *big.Int

var cfgOnce sync.Once

func init() {
	mustInit(gmajcfg.DefaultConfig)
	config.Log.Println("something")
}

// Init allows consumers of this package to set the configuration. This has to
// happen before any other functionality of the package is used.
func Init(cfg *gmajcfg.Config) error {
	if err := setConfig(cfg); err != nil {
		return err
	}

	max = getMax()

	return nil
}

// setConfig sets the configuration for the whole package. Should only be called once
func setConfig(cfg *gmajcfg.Config) error {
	var called bool
	var err error
	cfgOnce.Do(func() {
		defer func() { called = true }()

		if err = config.Validate(); err != nil {
			return
		}

		config = *cfg
	})

	if called {
		return err
	}

	return errSetConfig
}

func mustInit(cfg *gmajcfg.Config) {
	if err := cfg.Validate(); err != nil {
		config.Log.Fatalf("error setting configuration: %v", err)
	}
	config = *cfg
	max = getMax()
}

func getMax() *big.Int {
	max := big.NewInt(2)

	b2 := big.NewInt(2)
	for i := 0; i < config.KeySize; i++ {
		max.Mul(max, b2)
	}

	return max
}

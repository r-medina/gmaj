package gmaj

import (
	"errors"
	"log"
	"math/big"
	"sync"

	"github.com/r-medina/gmaj/gmajcfg"
)

var errSetConfig = errors.New("cannot set configuration more than once")

// the configuration for whole package
var cfg gmajcfg.Config

// the largest possible ID value
var max *big.Int

var cfgOnce sync.Once

func init() {
	if err := SetConfig(gmajcfg.DefaultConfig); err != nil {
		log.Fatalf("error setting configuration: %v", err)
	}

	max = func() *big.Int {
		max := big.NewInt(2)

		b2 := big.NewInt(2)
		for i := 0; i < cfg.KeySize; i++ {
			max.Mul(max, b2)
		}

		return max
	}()
}

// SetConfig sets the configuration for the whole package. Should only be called once
func SetConfig(config *gmajcfg.Config) error {
	var called bool
	var err error
	cfgOnce.Do(func() {
		defer func() { called = true }()

		if err = config.Validate(); err != nil {
			return
		}

		cfg = *config
	})

	if called {
		return err
	}

	return errSetConfig
}

package gmaj

import (
	"log"
	"math/big"

	"github.com/r-medina/gmaj/gmajcfg"
)

// the configuration for whole package
var cfg gmajcfg.Config

// the largest possible ID value
var max *big.Int

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

// SetConfig sets the configuration for the whole package.
func SetConfig(config *gmajcfg.Config) error {
	if err := config.Validate(); err != nil {
		return err
	}

	cfg = *config

	return nil
}

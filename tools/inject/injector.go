package inject

import (
	"context"
	"math/rand"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("inject")

var (
	WithKillTest      bool
	WithRollingUpdate bool
)

type Config struct {
	TotalReplicaCnt int    `json:"total_replica_count"`
	TotalMetaCnt    int    `json:"total_meta_count"`
	ScriptDir       string `json:"script_dir"`
	KillType        string `json:"kill_type"`
	ClusterName     string
	MetaServers     []string
}

type injector func(roundId int)

func Run(ctx context.Context, cfg *Config) {
	ru := NewRollingUpdater(cfg)
	kt := NewServerKillTest(cfg)

	var injectors []injector
	if WithKillTest {
		injectors = append(injectors, ru.Round)
	}
	if WithRollingUpdate {
		injectors = append(injectors, kt.Round)
	}

	roundId := 0
	for {
		roundId++

		inject := injectors[rand.Int31n(int32(len(injectors)))]
		inject(roundId)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

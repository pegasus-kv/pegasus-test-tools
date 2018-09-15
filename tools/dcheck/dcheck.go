package dcheck

import (
	"context"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/op/go-logging"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg        pegasus.Config             `json:"client"`
	RemoteCfg        pegasus.Config             `json:"remote_client"`
	SchemaCfg        tools.SchemaConfig         `json:"schema"`
	KillCfg          tools.KillTestConfig       `json:"kill"`
	RollingUpdateCfg tools.RollingUpdaterConfig `json:"rolling_update"`
}

var log = logging.MustGetLogger("dcheck")

func Run(rootCtx context.Context, withKillTest bool, withRollingUpdate bool) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-dcheck.json", cfg)

	v1 := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)
	v2 := tools.NewVerifier(cfg.RemoteCfg, &cfg.SchemaCfg, rootCtx)

	if withKillTest {
		kt := tools.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}
	if withRollingUpdate {
		ru := tools.NewRollingUpdater(cfg.RollingUpdateCfg, cfg.ClientCfg.MetaServers)
		go ru.Run(rootCtx)
	}

	hid := int64(0)
	masterHid := int64(0)     // [0, masterHid] are written on master
	duplicatedHid := int64(0) // [0, duplicatedHid] are duplicated.

	go tools.ProgressReport(rootCtx, "duplicated", 60*time.Second, &duplicatedHid, cfg.SchemaCfg.SortKeyBatch)

	go func() {
		sleepTime := rand.Intn(60) + 50
		for tools.WaitTil(rootCtx, time.Duration(sleepTime)*time.Second) {
			v1.FullScan(atomic.LoadInt64(&masterHid))
			sleepTime += rand.Intn(5) + 10
		}
	}()

	go func() {
		dataLatency := 120
		round := 0
		for tools.WaitTil(rootCtx, time.Duration(dataLatency)*time.Second) {
			mhid := atomic.LoadInt64(&hid)
			shid := duplicatedHid
			log.Infof("%s: round(%d) start verifying duplication [master(hid:%d), slave(hid:%d), latency(%ds)]", v2, round, mhid, shid, dataLatency)
			v2.FullScan(shid)

			// written data must arrive at remote cluster within the given data latency
			dataLatency = int(math.Max(float64(dataLatency+60), 600))
			duplicatedHid = atomic.LoadInt64(&hid)
			round++
		}
	}()

	for {
		v1.WriteBatchOrDie(hid)
		v1.ReadBatchOrDie(hid)

		// mark verified point
		atomic.StoreInt64(&masterHid, hid)

		atomic.AddInt64(&hid, 1)

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

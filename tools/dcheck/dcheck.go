package dcheck

import (
	"context"
	"sync/atomic"
	"time"

	"math/rand"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg        pegasus.Config             `json:"client"`
	RemoteCfg        pegasus.Config             `json:"remote_client"`
	SchemaCfg        tools.SchemaConfig         `json:"schema"`
	KillCfg          tools.KillTestConfig       `json:"kill"`
	RollingUpdateCfg tools.RollingUpdaterConfig `json:"rolling_update"`
}

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
	masterHid := int64(0)     // [0, masterHid] are be written on master
	duplicatedHid := int64(0) // [0, duplicatedHid] are duplicated.
	pendingHid := int64(0)
	lastVerifiedTs := time.Now()

	go tools.ProgressReport(rootCtx, "duplicated", 60*time.Second, &duplicatedHid, cfg.SchemaCfg.SortKeyBatch)

	go func() {
		sleepTime := rand.Intn(60) + 50
		for tools.WaitTil(rootCtx, time.Duration(sleepTime)*time.Second) {
			v1.FullScan(atomic.LoadInt64(&masterHid))
			sleepTime += rand.Intn(5) + 10
		}
	}()

	go func() {
		sleepTime := rand.Intn(60) + 50
		for tools.WaitTil(rootCtx, time.Duration(sleepTime)*time.Second) {
			v2.FullScan(atomic.LoadInt64(&duplicatedHid))
			sleepTime += rand.Intn(5) + 10
		}
	}()

	for {
		v1.WriteBatchOrDie(hid)
		v1.ReadBatchOrDie(hid)

		if time.Now().Sub(lastVerifiedTs) > time.Second*60 {
			if pendingHid > 0 {
				// written data must arrive at remote cluster within 60s
				atomic.StoreInt64(&duplicatedHid, pendingHid)
			}
			atomic.StoreInt64(&pendingHid, hid)
			lastVerifiedTs = time.Now()
		}

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

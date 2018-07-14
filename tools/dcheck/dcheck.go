package dcheck

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg pegasus.Config       `json:"client"`
	RemoteCfg pegasus.Config       `json:"remote_client"`
	SchemaCfg tools.SchemaConfig   `json:"schema"`
	KillCfg   tools.KillTestConfig `json:"kill"`
}

func Run(rootCtx context.Context, withKillTest bool) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-dcheck.json", cfg)

	v1 := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)
	v2 := tools.NewVerifier(cfg.RemoteCfg, &cfg.SchemaCfg, rootCtx)

	if withKillTest {
		kt := tools.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}

	hid := int64(0)
	verifiedHid := int64(0)
	duplicatedHid := int64(0) // [0, duplicated] are assumed to be duplicated.
	pendingHid := int64(0)
	lastVerifiedTs := time.Now()

	go tools.ProgressReport(rootCtx, "verify", &hid, cfg.SchemaCfg.SortKeyBatch)

	go func() {
		for {
			v1.FullScan(atomic.LoadInt64(&verifiedHid))
			tools.WaitTil(rootCtx, time.Second*60)
		}
	}()

	go func() {
		for {
			v2.FullScan(atomic.LoadInt64(&duplicatedHid))
			tools.WaitTil(rootCtx, time.Second*60)
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
		atomic.StoreInt64(&verifiedHid, hid)

		atomic.AddInt64(&hid, 1)

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

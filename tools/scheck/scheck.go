package scheck

import (
	"context"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
	"sync/atomic"
	"time"
)

type Config struct {
	ClientCfg pegasus.Config       `json:"client"`
	SchemaCfg tools.SchemaConfig   `json:"schema"`
	KillCfg   tools.KillTestConfig `json:"kill"`
}

func Run(rootCtx context.Context, withKillTest bool) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-scheck.json", cfg)

	v := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)

	hid := int64(0)
	verifiedHid := int64(0)

	if withKillTest {
		kt := tools.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}

	go tools.ProgressReport(rootCtx, "verify", time.Second*10, &hid, cfg.SchemaCfg.SortKeyBatch)

	// periodically verify the old data to ensure they are not lost.
	go func() {
		for tools.WaitTil(rootCtx, time.Second*60) {
			v.FullScan(atomic.LoadInt64(&verifiedHid))
		}
	}()

	for {
		v.WriteBatchOrDie(hid)
		v.ReadBatchOrDie(hid)

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

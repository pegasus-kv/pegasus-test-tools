package scheck

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
	"github.com/pegasus-kv/pegasus-test-tools/tools/inject"
)

type Config struct {
	ClientCfg pegasus.Config        `json:"client"`
	SchemaCfg tools.SchemaConfig    `json:"schema"`
	KillCfg   inject.KillTestConfig `json:"kill"`
}

func Run(rootCtx context.Context, withKillTest bool) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-scheck.json", cfg)

	client := pegasus.NewClient(cfg.ClientCfg)
	v := tools.NewVerifier(0, fmt.Sprintf("v-%s", cfg.ClientCfg.MetaServers[0]), client, &cfg.SchemaCfg, rootCtx)

	hid := int64(0)
	verifiedHid := int64(0)

	if withKillTest {
		kt := inject.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}

	go tools.ProgressReport(rootCtx, "verify", time.Second*10, &hid, cfg.SchemaCfg.SortKeyBatch)

	// periodically verify the old data to ensure they are not lost.
	go func() {
		sleepTime := rand.Intn(60) + 50
		for tools.WaitTil(rootCtx, time.Duration(sleepTime)*time.Second) {
			v.FullScan(atomic.LoadInt64(&verifiedHid))
			sleepTime += rand.Intn(5) + 10
		}
	}()

	for {
		v.WriteBatch(hid)
		v.ReadBatch(hid)

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

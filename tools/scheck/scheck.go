package scheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg pegasus.Config       `json:"client"`
	SchemaCfg tools.SchemaConfig   `json:"schema"`
	KillCfg   tools.KillTestConfig `json:"kill"`
}

func Run(rootCtx context.Context, withKillTest bool) {
	rand.Seed(time.Now().UnixNano())

	cfgPath, _ := filepath.Abs("config-scheck.json")
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Fatalln(err)
		return
	}

	cfg := &Config{}
	json.Unmarshal(rawCfg, cfg)

	v := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)

	hid := int64(0)
	verifiedHid := int64(0)

	if withKillTest {
		kt := tools.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}

	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 10):
				num := atomic.LoadInt64(&hid) * int64(cfg.SchemaCfg.SortKeyBatch)
				log.Printf("verified %d records in total", num)
			case <-rootCtx.Done():
				return
			}
		}
	}()

	// periodically verify the old data to ensure they are not lost.
	go func() {
		for {
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

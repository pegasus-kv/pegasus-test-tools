package scheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"time"
	"sync/atomic"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg pegasus.Config     `json:"client"`
	SchemaCfg tools.SchemaConfig `json:"schema"`
}

func Run(rootCtx context.Context) {
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

	for {
		v.WriteBatchOrDie(hid)
		v.ReadBatchOrDie(hid)

		atomic.AddInt64(&hid, 1)

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

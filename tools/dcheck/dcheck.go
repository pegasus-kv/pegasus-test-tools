package dcheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
)

type Config struct {
	ClientCfg pegasus.Config     `json:"client"`
	RemoteCfg pegasus.Config     `json:"remote_client"`
	SchemaCfg tools.SchemaConfig `json:"schema"`
}

func Run(rootCtx context.Context) {
	cfgPath, _ := filepath.Abs("config-dcheck.json")
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Fatalln(err)
		return
	}

	cfg := &Config{}
	json.Unmarshal(rawCfg, cfg)

	v1 := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)
	v2 := tools.NewVerifier(cfg.RemoteCfg, &cfg.SchemaCfg, rootCtx)

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
		v1.WriteBatchOrDie(hid)
		v1.ReadBatchOrDie(hid)

		time.Sleep(time.Second * 30)

		v2.ReadBatchOrDie(hid)

		atomic.AddInt64(&hid, 1)

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

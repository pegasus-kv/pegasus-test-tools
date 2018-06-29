package dcheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
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

	hid1 := int64(0)

	for {
		v1.WriteBatchOrDie(hid1)
		v1.ReadBatchOrDie(hid1)

		time.Sleep(time.Second * 30)

		v2.ReadBatchOrDie(hid1)

		hid1++

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

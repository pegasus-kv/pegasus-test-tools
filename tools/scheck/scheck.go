package scheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"

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

	for {
		v.WriteBatchOrDie(hid)
		v.ReadBatchOrDie(hid)

		hid++

		select {
		case <-rootCtx.Done():
			return
		default:
		}
	}
}

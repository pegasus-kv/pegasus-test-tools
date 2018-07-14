package bbench

import (
	"context"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
	"log"
)

type Config struct {
	ClientCfg   pegasus.Config     `json:"client"`
	SchemaCfg   tools.SchemaConfig `json:"schema"`
	RecordCount int                `json:"record_count"`
	WorkerCount int                `json:"worker_count"`
}

func Run(rootCtx context.Context, opType string) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-bbench.json", cfg)

	v := tools.NewVerifier(cfg.ClientCfg, &cfg.SchemaCfg, rootCtx)

	if opType == "load" {
		log.Printf("start loading (cnt: %d)", cfg.RecordCount)

		for w := 1; w <= cfg.WorkerCount; w++ {
			go func(w int) {
				split := cfg.RecordCount / cfg.WorkerCount
				if w == cfg.WorkerCount {
					split += cfg.RecordCount % cfg.WorkerCount
				}
				startHid := cfg.RecordCount / cfg.WorkerCount * w

				for i := 0; i < split; i++ {
					v.WriteBatchOrDie(int64(i + startHid))
				}
			}(w)
		}
		return
	}

	if opType == "run" {
		log.Printf("start executing (cnt: %d)", cfg.RecordCount)
		for w := 1; w <= cfg.WorkerCount; w++ {
			go func(w int) {
				split := cfg.RecordCount / cfg.WorkerCount
				if w == cfg.WorkerCount {
					split += cfg.RecordCount % cfg.WorkerCount
				}
				startHid := cfg.RecordCount / cfg.WorkerCount * w

				for i := 0; i < split; i++ {
					v.ReadBatchOrDie(int64(i + startHid))
				}
			}(w)
		}
		return
	}

}

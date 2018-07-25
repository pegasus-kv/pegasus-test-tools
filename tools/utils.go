package tools

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync/atomic"
	"time"
)

func LoadAndUnmarshalConfig(filePath string, cfg interface{}) {
	cfgPath, _ := filepath.Abs(filePath)
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Fatalln(err)
		return
	}

	json.Unmarshal(rawCfg, cfg)
}

// Periodically reports the current progress of `action`.
func ProgressReport(rootCtx context.Context, prefix string, period time.Duration, id *int64, recordsPerId int) {
	for {
		select {
		case <-time.Tick(period):
			num := atomic.LoadInt64(id) * int64(recordsPerId)
			log.Printf("%s: %d records in total", prefix, num)
		case <-rootCtx.Done():
			return
		}
	}
}

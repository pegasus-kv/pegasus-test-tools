package dcheck

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/op/go-logging"
	"github.com/pegasus-kv/pegasus-test-tools/tools"
	"github.com/pegasus-kv/pegasus-test-tools/tools/inject"
)

type Config struct {
	LocalCfg         tools.ClientConfig          `json:"local_client"`
	RemoteCfg        tools.ClientConfig          `json:"remote_client"`
	SchemaCfg        tools.SchemaConfig          `json:"schema"`
	KillCfg          inject.KillTestConfig       `json:"kill"`
	RollingUpdateCfg inject.RollingUpdaterConfig `json:"rolling_update"`
	Workers          int                         `json:"workers"`
}

var log = logging.MustGetLogger("dcheck")

func Run(rootCtx context.Context, withKillTest bool, withRollingUpdate bool) {
	cfg := &Config{}
	tools.LoadAndUnmarshalConfig("config-dcheck.json", cfg)
	if cfg.Workers <= 0 {
		log.Fatalf("invalid number of verifier workers: %d", cfg.Workers)
	}

	// global client object
	localCfg := pegasus.Config{MetaServers: cfg.LocalCfg.MetaServers}
	remoteCfg := pegasus.Config{MetaServers: cfg.RemoteCfg.MetaServers}
	localClient := pegasus.NewClient(localCfg)
	remoteClient := pegasus.NewClient(remoteCfg)

	if withRollingUpdate {
		ru := inject.NewRollingUpdater(cfg.RollingUpdateCfg, cfg.LocalCfg)
		go ru.Run(rootCtx)
	}
	if withKillTest {
		kt := inject.NewServerKillTest(&cfg.KillCfg)
		go kt.Run(rootCtx)
	}

	for i := 0; i < cfg.Workers; i++ {
		// distribute the workers into different time ranges.
		time.Sleep(time.Duration(time.Second))

		id := i
		go func() {
			v1 := tools.NewVerifier(id, fmt.Sprintf("v-%s-i%d", cfg.LocalCfg.ClusterName, id), localClient, &cfg.SchemaCfg, rootCtx)
			v2 := tools.NewVerifier(id, fmt.Sprintf("v-%s-i%d", cfg.RemoteCfg.ClusterName, id), remoteClient, &cfg.SchemaCfg, rootCtx)

			hid := int64(0)
			masterHid := int64(0)     // [0, masterHid] are written on master
			duplicatedHid := int64(0) // [0, duplicatedHid] are duplicated.

			go tools.ProgressReport(rootCtx, "duplicated", 60*time.Second, &duplicatedHid, cfg.SchemaCfg.SortKeyBatch)

			go func() {
				sleepTime := rand.Intn(60) + 50
				for tools.WaitTil(rootCtx, time.Duration(sleepTime)*time.Second) {
					v1.FullScan(atomic.LoadInt64(&masterHid))
					sleepTime += rand.Intn(5) + 10
				}
			}()

			go func() {
				dataLatency := 120
				round := 0
				for tools.WaitTil(rootCtx, time.Duration(dataLatency)*time.Second) {
					mhid := atomic.LoadInt64(&hid)
					shid := duplicatedHid
					log.Infof("%s: round(%d) start verifying duplication [master(hid:%d), slave(hid:%d), latency(%ds)]",
						v2, round, mhid, shid, dataLatency)
					v2.FullScan(shid)

					// written data must arrive at remote cluster within the given data latency
					dataLatency = int(math.Max(float64(dataLatency+60), 600))
					duplicatedHid = atomic.LoadInt64(&hid)
					round++
				}
			}()

			for {
				v1.WriteBatch(hid)
				v1.ReadBatch(hid)

				// mark verified point
				atomic.StoreInt64(&masterHid, hid)

				atomic.AddInt64(&hid, 1)

				select {
				case <-rootCtx.Done():
					return
				default:
				}
			}
		}()
	}

	<-rootCtx.Done()
}

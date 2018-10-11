package tools

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"time"
)

type RollingUpdaterConfig struct {
	ScriptDir string `json:"script_dir"`
}

type RollingUpdater struct {
	cfg       RollingUpdaterConfig
	clientCfg ClientConfig
}

func NewRollingUpdater(config RollingUpdaterConfig, clientConfig ClientConfig) *RollingUpdater {
	return &RollingUpdater{cfg: config, clientCfg: clientConfig}
}

func (r *RollingUpdater) Run(ctx context.Context) {
	roundId := 0
	for {
		roundId++
		r.round(roundId)

		select {
		case <-ctx.Done():
			log.Info("stopping rolling updater")
			return
		default:
		}
	}
}

func (r *RollingUpdater) round(roundId int) {
	// sleep for a random time before rolling update
	sleepTime := int(math.Min(float64(60+20*roundId), 1000))
	log.Infof("sleep %ds before kill", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	metaServers := strings.Join(r.clientCfg.MetaServers, ",")
	cmdStr := fmt.Sprintf("cd %s; ./pegasus_rolling_update.sh %s %s all 0", r.cfg.ScriptDir, r.clientCfg.ClusterName, metaServers)
	log.Infof("execute shell command \"%s\"", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	output, err := cmd.Output()
	log.Critical(string(output))
	if err != nil {
		log.Fatal(err)
	}
}

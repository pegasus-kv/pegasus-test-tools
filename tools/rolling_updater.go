package tools

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"time"
)

type RollingUpdaterConfig struct {
	ScriptDir   string `json:"script_dir"`
	ClusterName string `json:"cluster_name"`
}

type RollingUpdater struct {
	cfg         RollingUpdaterConfig
	metaServers string
}

func NewRollingUpdater(config RollingUpdaterConfig, metaServers []string) *RollingUpdater {
	return &RollingUpdater{cfg: config, metaServers: strings.Join(metaServers, ",")}
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
	sleepTime := rand.Intn(120) + 60
	log.Infof("sleep %ds before kill", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	cmdStr := fmt.Sprintf("cd %s; ./deploy rolling_update pegasus %s --skip_confirm --time_interval 10 --job replica meta", r.cfg.ScriptDir, r.cfg.ClusterName)
	log.Infof("execute shell command \"%s\"", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	output, err := cmd.Output()
	log.Info(string(output))
	if err != nil {
		log.Fatal(err)
	}
}

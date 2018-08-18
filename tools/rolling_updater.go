package tools

import (
	"context"
	"fmt"
	"log"
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
			log.Printf("stopping rolling updater")
			return
		default:
		}
	}
}

func (r *RollingUpdater) round(roundId int) {
	// sleep for a random time before rolling update
	sleepTime := rand.Intn(60) + 60
	log.Printf("sleep %ds before kill", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	cmdStr := fmt.Sprintf("cd %s; bash pegasus_rolling_update.sh %s %s all 0", r.cfg.ScriptDir, r.cfg.ClusterName, r.metaServers)
	log.Printf("execute shell command \"%s\"", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	output, err := cmd.Output()
	log.Printf(string(output))
	if err != nil {
		log.Fatal(err)
	}
}

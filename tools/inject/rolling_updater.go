package inject

import (
	"fmt"
	"math"
	"os/exec"
	"strings"
	"time"
)

type RollingUpdater struct {
	cfg *Config
}

func NewRollingUpdater(config *Config) *RollingUpdater {
	return &RollingUpdater{cfg: config}
}

func (r *RollingUpdater) Round(roundId int) {
	// sleep for a random time before rolling update
	sleepTime := int(math.Min(float64(60+20*roundId), 1000))
	log.Infof("sleep %ds before rolling update", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	metaServers := strings.Join(r.cfg.MetaServers, ",")
	cmdStr := fmt.Sprintf("cd %s/scripts; ./pegasus_rolling_update.sh %s %s all 0",
		r.cfg.ScriptDir, r.cfg.ClusterName, metaServers)
	log.Infof("execute shell command \"%s\"", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	output, err := cmd.Output()
	log.Critical(string(output))
	if strings.Contains(string(output), "extract replica count from perf counters failed") {
		// ignore error from collector
		return
	}
	if strings.Count(string(output), "Rolling updating replica success") != r.cfg.TotalReplicaCnt {
		log.Fatal(err)
	}
	if !strings.Contains(string(output), "Rolling updating meta success") {
		log.Fatal(err)
	}
	if err != nil {
		log.Error(err)
	}
}

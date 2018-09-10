package tools

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	iTypeReplica = "r"
	iTypeMeta    = "m"
)

type KillTestConfig struct {
	RunScriptDir    string `json:"run_script_dir"`
	TotalReplicaCnt int    `json:"total_replica_count"`
	TotalMetaCnt    int    `json:"total_meta_count"`
}

// Randomly pick one replica server, kill it and restart it.
type ServerKillTest struct {
	cfg *KillTestConfig
	oc  *oneboxController
}

func NewServerKillTest(cfg *KillTestConfig) *ServerKillTest {
	s := &ServerKillTest{}
	s.cfg = cfg

	path, err := filepath.Abs(cfg.RunScriptDir)
	if err != nil {
		log.Fatalf("bad run script path: %s", path)
	}
	if _, err := os.Stat(path + "/run.sh"); os.IsNotExist(err) {
		log.Fatalf("run script path doesn't exist: %s", path+"/run.sh")
	}
	s.oc = &oneboxController{runScriptPath: path}

	return s
}

func (s *ServerKillTest) Run(ctx context.Context) {
	roundId := 0
	for {
		roundId++
		s.round(roundId)

		select {
		case <-ctx.Done():
			log.Info("stopping killer")
			return
		default:
		}
	}
}

func (s *ServerKillTest) round(roundId int) {
	// sleep for a random time before kill
	sleepTime := rand.Intn(60) + 60
	log.Infof("sleep %ds before kill", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	id := rand.Intn(s.cfg.TotalReplicaCnt) + 1
	log.Infof("killing replica %d at round %d", id, roundId)
	if err := s.oc.killInstance(iTypeReplica, id); err != nil {
		log.Fatalf("failed to kill replica %d: %s", id, err)
	}

	// sleep for a random time before restart
	sleepTime = rand.Intn(60) + 1
	log.Infof("sleep %ds before restart", sleepTime)
	time.Sleep(time.Second * time.Duration(sleepTime))

	log.Infof("restarting replica %d at round %d", id, roundId)
	if err := s.oc.startInstance(iTypeReplica, id); err != nil {
		log.Fatalf("failed to recover replica %d: %s", id, err)
	}
}

// Using shell (run.sh) to control onebox instance.
type oneboxController struct {
	runScriptPath string
}

func (o *oneboxController) killInstance(itype string, idx int) error {
	return o.control(itype, idx, "stop")
}

func (o *oneboxController) startInstance(itype string, idx int) error {
	return o.control(itype, idx, "start")
}

func (o *oneboxController) control(itype string, idx int, op string) error {
	cmdStr := fmt.Sprintf("cd %s; bash run.sh %s_onebox_instance -%s %d",
		o.runScriptPath, op, itype, idx)
	log.Infof("execute shell command \"%s\"", cmdStr)
	cmd := exec.Command("bash", "-c", cmdStr)

	output, err := cmd.Output()
	log.Info(string(output))
	return err
}

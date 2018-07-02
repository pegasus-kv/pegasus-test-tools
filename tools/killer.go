package tools

import (
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"time"
)

type KillTestConfig struct {
	RunScriptPath   string `json:"run_script_path"`
	TotalReplicaCnt int    `json:"total_replica_count"`
}

// Randomly pick one replica server, kill it and restart it.
type ServerKillTest struct {
	cfg *KillTestConfig
	oc  *oneboxController
}

func NewServerKillTest(cfg *KillTestConfig) *ServerKillTest {
	s := &ServerKillTest{}
	s.cfg = cfg
	s.oc = &oneboxController{runScriptPath: cfg.RunScriptPath}
	return s
}

func (s *ServerKillTest) Round() {
	id := rand.Intn(s.cfg.TotalReplicaCnt)
	log.Printf("killing replica %d", id)

	if err := s.oc.killInstance("replica", id); err != nil {
		log.Fatalf("failed to kill replica %d: %s", id, err)
	}

	// sleep for a random time before restart
	time.Sleep(time.Second*time.Duration(rand.Intn(60)) + 1)

	if err := s.oc.startInstance("replica", id); err != nil {
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
	cmdStr := fmt.Sprintf("cd %s; bash run.sh %s_onebox_instance --%s %d",
		o.runScriptPath, op, itype, idx)
	cmd := exec.Command("bash", "-c", cmdStr)
	return cmd.Run()
}

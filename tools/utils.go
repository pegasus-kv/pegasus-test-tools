package tools

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
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

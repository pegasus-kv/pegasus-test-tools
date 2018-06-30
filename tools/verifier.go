package tools

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/XiaoMi/pegasus-go-client/pegasus2"
)

type SchemaConfig struct {
	HashKeyPrefix string `json:"hash_key_prefix"`
	SortKeyPrefix string `json:"sort_key_prefix"`
	SortKeyBatch  int    `json:"sort_key_batch"`
	ValueSize     int    `json:"value_size"`
	AppName       string `json:"app_name"`
}

type Verifier struct {
	c  *pegasus2.Client
	tb pegasus.TableConnector

	schema *SchemaConfig

	rootCtx   context.Context
	opTimeout time.Duration
}

func NewVerifier(clientCfg pegasus.Config, schemaCfg *SchemaConfig, rootCtx context.Context) *Verifier {
	v := new(Verifier)

	v.opTimeout = time.Millisecond * 100
	v.c = pegasus2.NewClient(clientCfg)
	v.rootCtx = rootCtx

	var err error
	ctx, _ := context.WithTimeout(rootCtx, v.opTimeout)

	v.tb, err = v.c.OpenTable(ctx, schemaCfg.AppName)
	if err != nil {
		log.Fatalln(err)
	}

	v.schema = schemaCfg
	return v
}

func (v *Verifier) setOrDie(hashKey []byte, sortKey []byte, value []byte) {
	var err error
	for tries := 0; tries < 10; tries++ {
		ctx, _ := context.WithTimeout(context.Background(), v.opTimeout)
		err = v.tb.Set(ctx, hashKey, sortKey, value)
		if err == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	log.Fatalln(err)
}

// TODO(wutao1): use scan instead.
func (v *Verifier) getOrDie(hashKey []byte, sortKey []byte) (value []byte) {
	var err error
	for tries := 0; tries < 10; tries++ {
		ctx, _ := context.WithTimeout(context.Background(), v.opTimeout)
		value, err = v.tb.Get(ctx, hashKey, sortKey)
		if err == nil {
			return
		}
		if value == nil {
			log.Fatalf("can't find hashkey: %s, sortkey: %s", string(hashKey), string(sortKey))
			return // unreachable
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	log.Fatalln(err)
	return // unreachable
}

func (v *Verifier) generateKeyRange(hid int64) (hashKey []byte, sortKeys [][]byte) {
	hashKey = []byte(fmt.Sprintf("%s_%v", v.schema.HashKeyPrefix, hid))

	for sid := 0; sid < v.schema.SortKeyBatch; sid++ {
		var sidWithLeadingZero bytes.Buffer
		sidBuf := []byte(fmt.Sprintf("%v", sid))
		for i := 0; i < 20-len(sidBuf); i++ {
			sidWithLeadingZero.WriteByte('0')
		}
		sidWithLeadingZero.Write(sidBuf)
		sortKey := append([]byte(v.schema.SortKeyPrefix), sidWithLeadingZero.Bytes()...)

		sortKeys = append(sortKeys, sortKey)
	}

	return
}

// TODO(wutao1): write using multiple goroutines
func (v *Verifier) WriteBatchOrDie(hid int64) {
	value := &bytes.Buffer{}
	for vid := 0; vid < v.schema.ValueSize; vid++ {
		value.WriteByte('0')
	}

	hashKey, sortKeys := v.generateKeyRange(hid)
	for _, sortKey := range sortKeys {
		v.setOrDie(hashKey, sortKey, value.Bytes())

		select {
		case <-v.rootCtx.Done():
			return
		default:
		}
	}
}

func (v *Verifier) ReadBatchOrDie(hid int64) {
	hashKey, sortKeys := v.generateKeyRange(hid)
	for _, sortKey := range sortKeys {
		v.getOrDie(hashKey, sortKey)

		select {
		case <-v.rootCtx.Done():
			return
		default:
		}
	}
}

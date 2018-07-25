package tools

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

type SchemaConfig struct {
	HashKeyPrefix string `json:"hash_key_prefix"`
	SortKeyPrefix string `json:"sort_key_prefix"`
	SortKeyBatch  int    `json:"sort_key_batch"`
	ValueSize     int    `json:"value_size"`
	AppName       string `json:"app_name"`
}

type Verifier struct {
	clientCfg *pegasus.Config
	c         pegasus.Client
	tb        pegasus.TableConnector

	schema *SchemaConfig

	rootCtx   context.Context
	opTimeout time.Duration
}

func NewVerifier(clientCfg pegasus.Config, schemaCfg *SchemaConfig, rootCtx context.Context) *Verifier {
	v := new(Verifier)

	v.opTimeout = time.Second * 1
	v.c = pegasus.NewClient(clientCfg)
	v.clientCfg = &clientCfg
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
	for tries := 0; tries < 30; tries++ {
		ctx, _ := context.WithTimeout(v.rootCtx, v.opTimeout)
		if err = v.tb.Set(ctx, hashKey, sortKey, value); err != nil {
			log.Printf("%s [hashkey: %s, sortkey: %s]", err, hashKey, sortKey)
			time.Sleep(1 * time.Second)

			// check if cancelled
			select {
			case <-v.rootCtx.Done():
				return
			default:
			}

			// retry
			continue
		}

		// success
		return
	}

	log.Fatalln(err)
}

func (v *Verifier) getOrDie(hashKey []byte, sortKey []byte) (value []byte) {
	var err error
	for tries := 0; tries < 30; tries++ {
		ctx, _ := context.WithTimeout(v.rootCtx, v.opTimeout)
		if value, err = v.tb.Get(ctx, hashKey, sortKey); err != nil {
			log.Printf("%s [hashkey: %s, sortkey: %s]", err, hashKey, sortKey)
			time.Sleep(1 * time.Second)

			// check if cancelled
			select {
			case <-v.rootCtx.Done():
				return
			default:
			}

			// retry
			continue
		}

		// pegasus promises read-after-write consistency
		if value == nil {
			log.Fatalf("can't find record: [hashkey: %s, sortkey: %s]", string(hashKey), string(sortKey))
			// unreachable
		}

		// success
		return
	}

	log.Fatalln(err)
	return // unreachable
}

func (v *Verifier) generateKeyRange(hid int64) (hashKey []byte, sortKeys [][]byte) {
	hashKey = []byte(fmt.Sprintf("%s%d", v.schema.HashKeyPrefix, hid))

	for sid := 0; sid < v.schema.SortKeyBatch; sid++ {
		var sidWithLeadingZero bytes.Buffer
		sidBuf := []byte(fmt.Sprintf("%d", sid))
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
// Not thread-safe.
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

// Not thread-safe.
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

// Full scan the entire table to ensure data with hashKeys ranging in [0, hid)
// are not lost.
func (v *Verifier) FullScan(hid int64) {
	for i := int64(0); i < hid; i++ {
		// TODO(wutao1): use scan instead.
		v.ReadBatchOrDie(hid)

		select {
		case <-v.rootCtx.Done():
			return
		default:
		}
	}

	log.Printf("full scan complete [hid: %d, meta: %s]", hid, v.clientCfg.MetaServers[0])
}

func WaitTil(ctx context.Context, duration time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(duration):
		return true
	}
}

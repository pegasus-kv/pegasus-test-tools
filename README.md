
# Pegasus Test Tools

This project includes a set of tools that are useful for availability tests of pegasus.

## Installation

First ensure you have golang (>= 1.12) installed on your computer.

```
git clone https://github.com/pegasus-kv/pegasus-test-tools.git
cd pegasus-test-tools
make
```

Then the binary along with configurations will be placed under directory `/bin`.

## SCheck

Single cluster correctness checker.

**Usage:**

First ensure onebox (5-replica is recommended) is running on your computer,
because currently we only support kill/start process through 
`./run.sh start/stop_onebox_instance`

```
make
./bin/toolbox scheck
```

Configuration config-scheck.json must be placed at the same directory as toolbox:

```
{
  "client": {
    "meta_servers": [ // address of pegasus cluster
      "127.0.0.1:34601",
      "127.0.0.1:34602",
      "127.0.0.1:34603"
    ]
  },
  "schema": {
    "hash_key_prefix": "pegasus_test",
    "sort_key_prefix": "pegasus_test",
    "sort_key_batch": 1000,
    "value_size": 100,
    "app_name": "temp"
  },
  "kill": { // configuration for kill test
    "run_script_dir": "/home/mi/git/pegasus",
    "total_replica_count": 5
  }
}
```

By default scheck doesn't enable kill test. To randomly kill and restart servers in the background,
using the following command:

```
./bin/toolbox scheck -k
 or
./bin/toolbox scheck --kill
```

## DCheck

Duplication correctness checker.

**Usage:**

First place config-dcheck.json under the same path of `toolbox`, then run

```
./bin/toolbox dcheck
```



# Pegasus Test Tools

This project includes a set of tools that are useful for availability tests of pegasus.

## SCheck

Single cluster correctness checker.

**Usage:**

```
make
./bin/toolbox scheck
```

Configuration config-scheck.json must be placed at the same directory as toolbox:

```json
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

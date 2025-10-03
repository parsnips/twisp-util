Twisp Util
------------------

Some useful utilities when running with Twisp local

## backup twisp local

Run the following bash script which depends on `jq` and the `awscli` (version 2) installed:

```bash
#!/usr/bin/env bash

set -euo pipefail

endpoint=$1
file=$2

b=$(mktemp)

next=''
while [ "$next" != 'null' ]; do
    if [ "$next" == '' ]; then
        aws dynamodb scan --table-name '0a5ccc1d-7ac0-4efb-818b-d845b3a82165' --endpoint-url "$endpoint" > "$b"
    else
        aws dynamodb scan --table-name '0a5ccc1d-7ac0-4efb-818b-d845b3a82165' --starting-token "$next" --endpoint-url "$endpoint" > "$b"
    fi
    jq -rc '.Items[] | {Item: .}' "$b" >> "$file" 
    next=$(jq '.NextToken' "$b")
done

rm "$b"
```

Usage:

```bash
./backup.sh <endpoint> <backupfile>
```

example:

```bash
./backup.sh http://localhost:8082/ ~/tmp/backup.jsonl
```


## cmd/restore

In `/cmd/restore` you can backup a raw ndjson dynamo file into a running twisp instance:

To build, `go build ./cmd/restore/...` and copy the `restore` program to somewhere on your `PATH`.

usage:
```bash
./restore -endpoint http://localhost:8082/ -from-twisp-account 000000000000 -to-twisp-account test -file ~/tmp/backup.jsonl
```

will restore the backup into the default account `000000000000` into the account `test`

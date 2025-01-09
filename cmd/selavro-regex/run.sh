#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

_install(){
	echo avro2jsons missing.
	echo avro2jsons is available here: github.com/takanoriyanagitani/go-avro2jsons
	echo install command sample: go install -v url/to/command/dir@latest
	exit 1
}

#genavro

export ENV_COLUMN_PATTERN_REGEXP='^[ah]'
export ENV_COLUMN_PATTERN_REGEXP='^[hn]'

export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

which jq | fgrep -q jq || exec sh -c 'echo jq missing.; exit 1'
which avro2jsons | fgrep -q avro2jsons || _install

cat sample.d/sample.avro |
	./selavro-regex |
	avro2jsons |
	jq -c

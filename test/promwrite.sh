#!/bin/bash
# Execute inside promremotecli folder
# see https://github.com/m3dbx/prometheus_remote_client_golang
rand=`awk -v min=1 -v max=10 'BEGIN{srand(); print int(min+rand()*(max-min+1))}'`
dd=`date +%s`
go run main.go -u http://localhost:3100/api/v1/prom/remote/write -t http:metrics -d $dd,$rand

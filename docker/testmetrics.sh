#!/bin/bash
## Test Metrics for Loki/cLoki

        DOMAINS=("up" "down" "left" "right")

        for i in `seq 1 10`;
        do
                TIME=$(date --utc +%FT%T.%3NZ)
                RANDOM=$$$(date +%s)
                NAME=${DOMAINS[$RANDOM % ${#DOMAINS[@]}]}
                echo "$NAME, $TIME"
                curl  --header "Content-Type: application/json"  --request POST \
                        --data '{"streams": [{"labels": "{foo=\"bar\",name=\"'"$NAME"'\"}","entries": [{"ts": "'"$TIME"'", "line": "level=info string='"$RANDOM"'" }]}]}' \
                        'http://loki/api/prom/push' &
        done

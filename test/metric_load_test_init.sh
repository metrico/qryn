#!/bin/bash

# Partially prepare of Billy test for cLoki
# HOW TO RUN
# chmod +x metric_load_test_init.sh
# CLICKHOUSE_DB=db_name SENSORS=sensors_number CHECKS=amount_of_ticks_for_sensor MS=time_between_two_ticks_in_ms \
# ./metric_load_test_init.sh all_flags_for_clickhouse-client_to_connect_to_db

if [ -z $SENSORS ]; then export SENSORS=1000; fi
if [ -z $CHECKS ]; then export CHECKS=3600; fi
if [ -z $MS ]; then export MS=1000; fi
echo "INSERT TIME SERIES"
echo "INSERT INTO $CLICKHOUSE_DB.time_series (date, fingerprint, labels) SELECT toStartOfDay(now()), number, format('{{\"test_id\":\"LOAD_TEST\", \"id\":\"{0}\"}}', toString(number)) FROM numbers(1000)"
clickhouse-client $@ -q "INSERT INTO $CLICKHOUSE_DB.time_series (date, fingerprint, labels) SELECT toStartOfDay(now()), number, format('{{\"test_id\":\"LOAD_TEST\", \"id\":\"{0}\"}}', toString(number)) FROM numbers($SENSORS)" -t
echo "INSERT TIME SERIES OK"
echo "INSERT SAMPLES"
REQ="INSERT INTO $CLICKHOUSE_DB.samples_v2 (fingerprint, timestamp_ms, value) WITH\
       toUInt64(toUnixTimestamp(NOW())) * 1000 - $CHECKS * $MS as start_time,\
       $SENSORS as num_sensors,\
       $CHECKS * $MS as num_ms,\
       ceil($CHECKS * $MS / 24 * 3600 * 1000) as num_days,\
       24*60 as num_minutes,\
       24*60*60*1000 as ms_in_day,\
       60*1000 as ms_in_min,\
       num_days * num_minutes as total_minutes\
     SELECT\
       number % num_sensors as sensor_id,\
       start_time + (intDiv(intDiv(number, num_sensors) * $MS as mils, ms_in_day) as day) * ms_in_day \
                  + (intDiv(mils % ms_in_day, ms_in_min) as minute)*ms_in_min \
                  + (mils % ms_in_min) time,\
       60 + 20*sin(cityHash64(sensor_id)) /* median deviation */\
       + 15*sin(2*pi()/num_days*day) /* seasonal deviation */  \
       + 10*sin(2*pi()/num_minutes*minute)*(1 + rand(1)%100/2000) /* daily deviation */ \
       as temperature\
     FROM numbers_mt($SENSORS * $CHECKS)\
     SETTINGS max_block_size=1048576;"
echo "$REQ"
clickhouse-client $@ -q "$REQ" -t
echo "INSERT SAMPLES OK"

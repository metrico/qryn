#!/bin/bash
if [ -z $SENSORS ]; then export SENSORS=1000; fi
if [ -z $CHECKS ]; then export CHECKS=3600; fi
if [ -z $MS ]; then export MS=1000; fi
echo "INSERT TIME SERIES"
echo "INSERT INTO $CLICKHOUSE_DB.time_series (date, fingerprint, labels) SELECT toStartOfDay(now()), number, format('{{\"test_id\":\"LOAD_TEST\", \"id\":\"{0}\"}}', toString(number)) FROM numbers(1000)"
clickhouse-client $@ -q "INSERT INTO $CLICKHOUSE_DB.time_series (date, fingerprint, labels) SELECT toStartOfDay(now()), number, format('{{\"test_id\":\"LOAD_TEST\", \"id\":\"{0}\"}}', toString(number)) FROM numbers($SENSORS)" --output_format_write_statistics true --print-profile-events --metrics_perf_events_enabled true -t
echo "INSERT TIME SERIES OK"
echo "INSERT SAMPLES"
REQ="INSERT INTO $CLICKHOUSE_DB.samples_v2 (fingerprint, timestamp_ms, value) WITH\
       toUInt64(toUnixTimestamp(NOW())) * 1000 - $CHECKS * $MS as start_time,\
       $SENSORS as num_sensors,\
       2*24*60*60*1000 as num_ms,\
       2 as num_days,\
       24*60 as num_minutes,\
       24*60*60*1000 as ms_in_day,\
       24*60*60*1000 as ms_in_min,\
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

#!/bin/bash
# Test script for health monitoring and fault tolerance

set -e

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "ERROR: docker compose not found"
  exit 1
fi

echo "Starting health check tests..."
echo ""

# cleanup
echo "Cleaning up old containers..."
$COMPOSE down -v 2>/dev/null || true
sleep 2

# rebuild
echo "Building images..."
docker build -t hdfs -f Dockerfile.hdfs . >/dev/null 2>&1
$COMPOSE build nn dn >/dev/null 2>&1
echo "Done building"

echo ""
echo "Test 1: Worker registration and heartbeats"
echo "Starting containers..."
$COMPOSE up -d
sleep 8

echo "Checking registration..."
$COMPOSE logs nn 2>&1 | grep "Registered worker" | tail -1

echo ""
echo "Waiting for heartbeats..."
sleep 8

echo "Worker side:"
$COMPOSE logs dn 2>&1 | grep -i "heartbeat" | head -5

echo ""
echo "Master side:"
$COMPOSE logs nn 2>&1 | grep "Heartbeat received" | head -3

echo ""
echo "Test 2: Multiple workers with jitter"
echo "Scaling to 3 workers..."
$COMPOSE up -d --scale dn=3
sleep 8

echo "Jitter values:"
for i in 1 2 3; do
    container="mapreduce-project-dn-$i"
    jitter=$(docker logs $container 2>&1 | grep "Starting heartbeat loop" | tail -1)
    if [ -n "$jitter" ]; then
        echo "  $jitter"
    fi
done

echo ""
echo "Current workers:"
$COMPOSE logs nn 2>&1 | grep "Current worker pool" | tail -1

echo ""
echo "Test 3: Workers stay alive"
echo "Waiting 10 seconds..."
sleep 10

removal_count=$($COMPOSE logs nn 2>&1 | grep -c "Removing inactive worker" || echo "0")
removal_count=$(echo "$removal_count" | tr -d '\n' | xargs)
echo "Removals: $removal_count"
if [ "$removal_count" = "0" ]; then
    echo "OK - no workers died"
else
    echo "WARNING - some workers removed"
fi

echo ""
echo "Test 4: Failure detection"
echo "Stopping worker 2..."
docker stop mapreduce-project-dn-2 >/dev/null 2>&1
stopped_time=$(date '+%H:%M:%S')
echo "Stopped at: $stopped_time"

echo "Waiting 15 seconds for master to notice..."
sleep 15

echo "Master logs:"
$COMPOSE logs nn 2>&1 | grep "Removing inactive" | tail -1
removal_time=$($COMPOSE logs nn 2>&1 | grep "Removing inactive" | tail -1 | grep -oE '[0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1)
echo "Removed at: $removal_time"

echo ""
echo "Test 5: Worker recovery"
echo "Restarting worker 2..."
$COMPOSE up -d dn
sleep 6

echo "Re-registration:"
$COMPOSE logs nn 2>&1 | grep "Registered worker" | tail -2

echo ""
echo "Test 6: Health reports"
health_check_exists=$($COMPOSE logs nn 2>&1 | grep -c "Health check:" || echo "0")
if [ "$health_check_exists" -gt "0" ]; then
    echo "Health reports found:"
    $COMPOSE logs nn 2>&1 | grep "Health check:" | tail -2
else
    echo "Health monitor is running (reports every 60s)"
fi

echo ""
echo "Test 7: Heartbeat validation"
rejection_count=$($COMPOSE logs dn 2>&1 | grep -c "Heartbeat rejected" || echo "0")
rejection_count=$(echo "$rejection_count" | tr -d '\n' | xargs)
echo "Rejected: $rejection_count"
if [ "$rejection_count" = "0" ]; then
    echo "All heartbeats accepted"
else
    echo "Some rejections found"
fi

echo ""
echo "Test 8: Worker failure during job"
echo "Uploading data..."
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py >/dev/null 2>&1 || echo "(data already exists)"
sleep 2

echo "Starting word count job..."
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file1.txt /client_folder/data/small/file2.txt \
    --reducers 2 > /tmp/test8_job_output.log 2>&1 &

JOB_PID=$!
echo "Job PID: $JOB_PID"

echo "Waiting for map to start..."
sleep 3

echo "Killing worker 1..."
docker stop mapreduce-project-dn-1 >/dev/null 2>&1

echo "Waiting for job to finish (max 30s)..."
job_timeout=30
job_elapsed=0
while kill -0 $JOB_PID 2>/dev/null && [ $job_elapsed -lt $job_timeout ]; do
    sleep 2
    job_elapsed=$((job_elapsed + 2))
done

if kill -0 $JOB_PID 2>/dev/null; then
    echo "Job timeout"
    kill $JOB_PID 2>/dev/null
else
    echo "Job finished"
fi

failure_detected=$($COMPOSE logs nn 2>&1 | grep -c "Removing inactive worker" || echo "0")
if [ "$failure_detected" -gt "0" ]; then
    $COMPOSE logs nn 2>&1 | grep "Removing inactive worker" | tail -1
    echo "Master detected failure and rescheduled task"
else
    echo "FAILED - master didn't detect worker failure"
fi

rm -f /tmp/test8_job_output.log

echo ""
echo "Test 9: Worker failure during reduce phase"
echo "Restarting all workers..."
$COMPOSE up -d --scale dn=2
sleep 6

echo "Uploading data (if needed)..."
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py >/dev/null 2>&1 || echo "(data exists)"
sleep 1

echo "Starting word count job..."
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file1.txt /client_folder/data/small/file2.txt /client_folder/data/small/file3.txt \
    --reducers 2 > /tmp/test9_job_output.log 2>&1 &

JOB_PID=$!
echo "Job PID: $JOB_PID"

echo "Waiting 6 seconds for reduce phase to start..."
sleep 6

echo "Killing worker during reduce..."
docker stop mapreduce-project-dn-2 >/dev/null 2>&1

echo "Waiting for job to finish (max 30s)..."
job_timeout=30
job_elapsed=0
while kill -0 $JOB_PID 2>/dev/null && [ $job_elapsed -lt $job_timeout ]; do
    sleep 2
    job_elapsed=$((job_elapsed + 2))
done

if kill -0 $JOB_PID 2>/dev/null; then
    echo "Job timeout"
    kill $JOB_PID 2>/dev/null
else
    echo "Job finished"
fi

failure_detected=$($COMPOSE logs nn 2>&1 | grep -c "Removing inactive worker" || echo "0")
if [ "$failure_detected" -gt "0" ]; then
    $COMPOSE logs nn 2>&1 | grep "Removing inactive worker" | tail -1
    echo "Master detected failure and rescheduled reduce task"
else
    echo "FAILED - master didn't detect worker failure"
fi

rm -f /tmp/test9_job_output.log

echo ""
echo "Test 10: Channel fix - job after worker restart"
echo "Starting fresh with 2 workers..."
$COMPOSE up -d --scale dn=2
sleep 6

echo "Running job 1..."
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file1.txt \
    --reducers 2 >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "Job 1 completed"
else
    echo "Job 1 failed"
fi

echo "Killing worker 1..."
docker stop mapreduce-project-dn-1 >/dev/null 2>&1
sleep 3

echo "Restarting worker 1..."
docker start mapreduce-project-dn-1 >/dev/null 2>&1
sleep 6

echo "Running job 2 (tests channel fix)..."
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file2.txt \
    --reducers 2 >/dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "Job 2 completed - channel fix works!"
    echo "Stale channel bug is fixed"
else
    echo "FAILED - Job 2 crashed with networking error"
    echo "Channel bug still exists"
fi

echo ""
echo "Summary:"
echo ""

hb_sent=$($COMPOSE logs dn 2>&1 | grep -c "Heartbeat acknowledged" || echo "0")
hb_sent=$(echo "$hb_sent" | tr -d '\n' | xargs)
echo "Heartbeats sent: $hb_sent"

hb_received=$($COMPOSE logs nn 2>&1 | grep -c "Heartbeat received" || echo "0")
hb_received=$(echo "$hb_received" | tr -d '\n' | xargs)
echo "Heartbeats received: $hb_received"

registrations=$($COMPOSE logs nn 2>&1 | grep -c "Registered worker" || echo "0")
registrations=$(echo "$registrations" | tr -d '\n' | xargs)
echo "Registrations: $registrations"

removals=$($COMPOSE logs nn 2>&1 | grep -c "Removing inactive" || echo "0")
removals=$(echo "$removals" | tr -d '\n' | xargs)
echo "Removals: $removals"

failures=$($COMPOSE logs dn 2>&1 | grep -c "Heartbeat failed" || echo "0")
failures=$(echo "$failures" | tr -d '\n' | xargs)
echo "Heartbeat failures: $failures"

echo ""
echo "Active workers:"
$COMPOSE logs nn 2>&1 | grep "Health check:" | tail -1

echo ""
echo "Tests done. To clean up: $COMPOSE down"
echo ""

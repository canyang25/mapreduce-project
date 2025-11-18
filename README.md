# MapReduce Project
Honors CS544 - Distributed MapReduce System

## What This Is
A distributed MapReduce implementation with:
- Master node (coordinates jobs)
- Worker nodes (execute map/reduce tasks)
- HDFS storage
- Fault tolerance (health monitoring)

## Quick Start

### 1. Build and Start
```bash
docker compose up -d
```

### 2. Upload Data
```bash
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py
```

### 3. Run a Job
```bash
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file1.txt \
            /client_folder/data/small/file2.txt \
    --reducers 2
```

### 4. Stop
```bash
docker compose down
```

## Test Health Monitoring
```bash
./test_health_check.sh
```

## Project Structure
- `master.py` - Coordinator
- `worker.py` - Task executors
- `client_folder/jobs/` - MapReduce jobs
- `client_folder/data/` - Test data
- `design.md` - Architecture details

## Team
Canyang Zhao, Sid Ganesh, Ryan Murphy, Anirudh Jagannath

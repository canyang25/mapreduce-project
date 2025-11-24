# Honors MapReduce Implementation

## Overview
This project implements a distributed MapReduce system using Docker Compose with fault tolerance features

## Team Members
- Canyang Zhao
- Sid Ganesh 
- Ryan Murphy 
- Anirudh Jagannath

## Architecture

### System Components
- **Client**: Python-based client for job submission
- **Master**: Lightweight master node for task coordination
- **Workers**: Multiple worker containers (max 4 cores) for map/reduce operations
- **Storage**: HDFS for distributed file storage
- **Communication**: gRPC for client-master-worker communication

### Project Structure
```
├── client_folder/      # Get's copied over into the client container 
│   ├── data/           # Data that can be used
│   ├── jobs/           # Map Reduce Jobs
│   └── scripts/       
├── docker-compose.yml  # Docker compose file - orchestrate all required containers
├── master.py           # All the code for the master node for the MapReduce implementation
└── worker.py           # All the code for the worker nodes for the MapReduce implementation
```

## Required Software
- Docker and Docker Compose

## Example Data
We include different size levels of data partitioned into separate directories. Small data help us with development and verifying basic functionality, large data gives us performance metrics and lets us test functionality at scale

## Example usage
High level:
- Upload Example Data
- Write a MapReduce Job
- Submit a Job
- Monitor Job Progress and view results

Specific:
- Upload any data you want to do a MapReduce job to client_folder/data
- Upload any MapReduce job to client_folder/jobs. It should follow standard MapReduce format: 
    - map (k1, v1) -> list(k2, v2)
    - reduce (k2, list(v2)) -> list(v2)
- Build the system
```bash
docker compose build
docker compose up -d
```
- Upload all data in client_folder/data to hdfs by running upload_data.py
```bash
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py
```
- Run MapReduce job (note if maps > number of input file paths, it defaults back to one map task per file)
```bash
docker compose exec client python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/<job file name> \
    --files /client_folder/data/<file 1> \
            /client_folder/data/<file 2> \
            ...
    --maps <number of maps>
    --reducers <number of reducers>
```
### Test Categories
1. **Health Tests**: Tests worker health checking (test_health_check.sh)
2. **Client-end Tests**: Tests end-to-end functionality by querying the system through different senarios

## Special Features
### Worker Health Monitoring
- Workers send heartbeats to master every ~5 seconds with jitter
- Master detects failed workers within 15 seconds and removes them from pool
- Periodic health status reports every 60 seconds
- Heartbeat response validation and detailed logging

**Testing:**
```bash
./test_health_check.sh  # Runs health-related test suite
```
After creating the containers
```bash
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py     # Uploads test data
docker exec mapreduce-project-client-1 python3 client_folder/scripts/test.py            # Runs client-side tests
```
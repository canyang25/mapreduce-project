# MapReduce Project

A MapReduce implementation built with Python, gRPC, and Docker, using HDFS for storage.

## Features
- **HDFS Integration**: Uses Hadoop Distributed File System for reliable data storage.
- **gRPC Communication**: Uses gRPC for communication between nodes.
- **Fault Tolerance**: Active health monitoring of workers with automatic failure detection.
- **Scalable Workers**: Docker-based worker nodes that can be scaled horizontally.
- **CLI Tool**: Python CLI for cluster management and job submission.

## 🏗️ Architecture
The system consists of four main components orchestrated via Docker Compose:
- **Master (`nn`)**: Coordinates job execution, manages worker registry, and handles task scheduling.
- **Workers (`dn`)**: Execute Map and Reduce tasks assigned by the Master. They report health status via heartbeats.
- **Client (`client`)**: Interface for users to submit jobs and upload data.


## 📋 Prerequisites
- **Docker** & **Docker Compose**
- **Python 3.x** (for the local CLI wrapper)

## ⚡ Quick Start

We provide a wrapper script `map_reduce.py` to simplify cluster interactions.
To learn more about the CLI tool, run:
```bash
./map_reduce.py help
```

### 1. Start the Cluster
Builds the Docker images and starts the services in the background.
```bash
./map_reduce.py start
```

### 2. Upload Test Data
Uploads local data from `client_folder/data` to HDFS.
```bash
./map_reduce.py upload_data
```

### 3. Run a MapReduce Job
Submit a job using the CLI.
```bash
# Syntax: ./map_reduce.py map_reduce <job_file> <input_files...> [--map MAP_FN --reduce REDUCE_FN --num-mappers N --num-reducers <N> --iterator ITERATOR_FN]

./map_reduce.py map_reduce word_count.py small/file1.txt small/file2.txt --num-reducers 2
```

### 4. Stop the Cluster
Stops and removes the containers.
```bash
./map_reduce.py stop
```

## 📝 Writing MapReduce Jobs
Jobs are Python files located in `client_folder/jobs/`. A job file should define `map` and `reduce` functions. Optionally, you can define an `iterator` function to customize how input files are split into records. If no iterator function is provided, each line is passed to map with an arbitraty key.

**Example (`word_count.py`):**
```python
def iterator(file_bytes, metadata):
    # Optional: Custom input splitting
    # file_bytes: raw file content (bytes)
    # metadata: dict with file info (e.g., {'file_path': '/data/file.txt'})
    
    # Default behavior (if not defined): yields (file_path, file_content_string)
    content = file_bytes.decode('utf-8')
    yield (metadata['file_path'], content)

def map(key, value):
    # key: document name (from iterator)
    # value: document contents (from iterator)
    for word in value.split():
        yield (word, 1)

def reduce(key, values):
    # key: word
    # values: list of counts
    yield (key, sum(values))
```

## 🛡️ Fault Tolerance
The system implements a heartbeat mechanism to ensure worker availability:
- **Heartbeats**: Workers send periodic heartbeats to the Master.
- **Failure Detection**: The Master maintains a registry of active workers. If a worker fails to heartbeat within a timeout period (default 15s), it is marked as dead and removed from the pool.
- **Testing**: Run `./test_health_check.sh` to verify failure detection logic.

## 📂 Project Structure
```
.
├── client_folder/          # Client resources
│   ├── data/               # Input text files
│   ├── jobs/               # Python MapReduce job definitions
│   └── scripts/            # Client implementation
├── master.py               # Master node implementation
├── worker.py               # Worker node implementation
├── map_reduce.py           # CLI wrapper script
├── *.proto                 # gRPC protocol definitions
├── Dockerfile.*            # Docker builds for components
├── docker-compose.yml      # Cluster orchestration
└── tests/                  # Test scripts
```

## 🐳 Manual Docker Commands (Alternative)
If you prefer using Docker Compose directly:

**Start:**
```bash
docker build -f Dockerfile.hdfs -t hdfs .
docker compose up -d --build
```

**Upload Data:**
```bash
docker exec mapreduce-project-client-1 python3 client_folder/scripts/upload_data.py
```

**Submit Job:**
```bash
docker exec mapreduce-project-client-1 python3 -m client_folder.scripts.interactive_client \
    --job /app/client_folder/jobs/word_count.py \
    --files /client_folder/data/small/file1.txt \
            /client_folder/data/small/file2.txt \
    --maps 2
    --reducers 2
```
**Logs:**
```bash
docker compose logs client, nn, dn -f
```

**Stop:**
```bash
docker compose down
```

## 👥 Team
- Canyang Zhao
- Sid Ganesh
- Ryan Murphy
- Anirudh Jagannath

# Honors MapReduce Implementation

## Overview
This project implements a distributed MapReduce system using Docker Compose with fault tolerance features. This is a living document which we will add to throughout development - right now it's very preliminary, mostly detailing the rough structure of this project including some basic design decisions.

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
├── scripts/          # .py implementations
├── test/             # Test suites
├── deploy/           # Docker Compose configuration
│   └── docker-compose.yml
├── proto/            # Protocol buffer definitions
└── examples/         # Example data and jobs
```

## Required Software
- Docker and Docker Compose
- Python 3.8+
- Protocol Buffer compiler (protoc)
- HDFS (Hadoop Distributed File System)

## Usage

Upload Example Data
Write a MapReduce Job
Submit a Job
Monitor Job Progress and view results

## Example Data
We plan to include different size levels of data partitioned into separate directories. Small data help us with development and verifying basic functionality, large data gives us performance metrics and lets us test functionality at scale

## Building system
[Further instructions for building the system and installing specific packages]

### Test Categories
1. **Unit Tests**: Individual component testing
2. **Integration Tests**: End-to-end workflow testing
3. **Performance Tests**: Scaling and throughput evaluation
4. **Fault Tolerance Tests**: Worker failure scenarios

## Special Features
### Worker Failure Handling
- Worker failure handling with periodic pings from master
- Exponential backoff + jitter to prevent thundering herd problem
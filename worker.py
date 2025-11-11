import os
import socket
import re
import time
import random
import logging
import grpc
import threading
from concurrent import futures

import worker_to_master_pb2
import worker_to_master_pb2_grpc

import master_to_worker_pb2
import master_to_worker_pb2_grpc

LOG = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

TASK_SERVER_PORT = 50051
WORKER_ID = socket.gethostname()


def register_with_retry(namenode_host, namenode_port, max_attempts=8, initial_delay=1.0):
    """Register this worker with the registry service. Retries with exponential backoff.

    Raises RuntimeError on unrecoverable failure.
    """
    container_id = socket.gethostname()

    LOG.info("Registering worker: id=%s", container_id)

    attempt = 0
    delay = initial_delay
    last_exc = None
    while attempt < max_attempts:
        attempt += 1
        try:
            with grpc.insecure_channel(f'{namenode_host}:{namenode_port}') as ch:
                stub = worker_to_master_pb2_grpc.RegistryStub(ch)
                resp = stub.Register(worker_to_master_pb2.WorkerInfo(id=container_id))
                if not resp.ok:
                    raise RuntimeError(f"Registration rejected: {resp.message}")
                LOG.info("Registered successfully as %s", container_id)
                return resp
        except Exception as e:
            last_exc = e
            LOG.warning("Registration attempt %d failed: %s", attempt, e)
            if attempt >= max_attempts:
                break
            time.sleep(delay)
            delay = min(delay * 2, 30)

    raise RuntimeError(f"Failed to register worker after {max_attempts} attempts") from last_exc


def heartbeat_loop(namenode_host: str, namenode_port: int, worker_id: str, interval_seconds: float = 5.0):
    """Continuously send heartbeat to master to indicate liveness.
    
    Uses jitter to prevent thundering herd problem when multiple workers heartbeat simultaneously.
    Runs indefinitely; logs warnings on failures but keeps retrying.
    """
    # Add initial random delay to spread out worker heartbeats (0-2 seconds)
    initial_jitter = random.uniform(0, 2.0)
    LOG.info("Starting heartbeat loop with %.2fs initial delay", initial_jitter)
    time.sleep(initial_jitter)
    
    while True:
        # Calculate next sleep time with jitter
        jitter = interval_seconds * 0.1 * (2 * random.random() - 1)
        sleep_time = interval_seconds + jitter
        
        try:
            with grpc.insecure_channel(f"{namenode_host}:{namenode_port}") as ch:
                stub = worker_to_master_pb2_grpc.RegistryStub(ch)
                response = stub.Heartbeat(worker_to_master_pb2.HeartbeatRequest(worker_id=worker_id))
                
                if not response.ok:
                    LOG.warning("Heartbeat rejected by master: %s", response.message)
                else:
                    LOG.debug("Heartbeat acknowledged by master (next in ~%.1fs)", sleep_time)
        except Exception as e:
            LOG.warning("Heartbeat failed: %s (will retry in ~%.1fs)", e, sleep_time)
        
        time.sleep(sleep_time)

class WorkerTaskServicer(master_to_worker_pb2_grpc.WorkerTaskServicer):
    def RunMap(self, request, context):
        LOG.info(f"Worker : {WORKER_ID} running map task {request.task_id}")
        # TODO: run mapper script
        return master_to_worker_pb2.Ack(ok=True, message="map done")

    def RunReduce(self, request, context):
        LOG.info(f"Worker : {WORKER_ID} running reduce task {request.task_id}")
        # TODO: run reducer script
        return master_to_worker_pb2.Ack(ok=True, message="reduce done")
    
def start_task_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    master_to_worker_pb2_grpc.add_WorkerTaskServicer_to_server(
        WorkerTaskServicer(), server
    )
    server.add_insecure_port(f"[::]:{TASK_SERVER_PORT}")
    server.start()
    LOG.info(f"Worker task server listening on {TASK_SERVER_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    try:
        resp = register_with_retry('boss', 8081)
        # Changed heartbeat to run in background thread
        # heartbeat_loop('boss', 8081, socket.gethostname())
        threading.Thread(
            target=heartbeat_loop,
            args=("boss", 8081, WORKER_ID),
            daemon=True
        ).start()
        # Start task server to accept tasks from master
        start_task_server()
    except Exception as e:
        LOG.error("Worker registration failed: %s", e)
        raise


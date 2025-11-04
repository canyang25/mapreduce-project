import os
import socket
import re
import time
import logging
import grpc

import worker_to_master_pb2
import worker_to_master_pb2_grpc

LOG = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


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


if __name__ == "__main__":
    try:
        register_with_retry('boss', 8081)
    except Exception as e:
        LOG.error("Worker registration failed: %s", e)
        raise

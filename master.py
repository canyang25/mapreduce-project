import os
from subprocess import check_output
import threading
import time
import logging
import grpc
from concurrent import futures
import pyarrow as pa
import pyarrow.fs
import docker

import master_client_pb2
import master_client_pb2_grpc

from google.protobuf import empty_pb2
import worker_to_master_pb2
import worker_to_master_pb2_grpc

# Set up logging
LOG = logging.getLogger("master")
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Set up HDFS classpath (optional - health checking doesn't need it)
try:
    os.environ["CLASSPATH"] = str(check_output([os.environ["HADOOP_HOME"]+"/bin/hdfs", "classpath", "--glob"]), "utf-8")
except Exception as e:
    LOG.warning("Could not set HDFS classpath (HDFS may not be ready): %s", e)


class MasterClientService(master_client_pb2_grpc.MasterClientServicer):
    def __init__(self):
        # Connect to HDFS using the namenode hostname (boss) and port
        # Make connection lazy to avoid startup failures if HDFS isn't ready
        self.hdfs_client = None
        try:
            LOG.info("Initializing MasterClientService with HDFS connection to boss:9000")
            self.hdfs_client = pa.fs.HadoopFileSystem(
                host='boss',
                port=9000
            )
        except Exception as e:
            LOG.warning("HDFS connection failed (will retry when needed): %s", e)
            # Health checking doesn't need HDFS, so we continue
    
    def MapReduce(self, request, context):
        LOG.info("Received MapReduce request")
        try:
            # TODO: Implement actual MapReduce logic
            return master_client_pb2.MapReduceResponse(
                success=True,
                file_paths=[]
            )
        except Exception as e:
            LOG.error("MapReduce failed: %s", str(e))
            return master_client_pb2.MapReduceResponse(
                success=False,
                file_paths=[]
            )


def make_master_server(bind):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_client_pb2_grpc.add_MasterClientServicer_to_server(MasterClientService(), server)
    server.add_insecure_port(bind)
    return server


class _State:
    """In-memory worker registry."""
    def __init__(self):
        self._lock = threading.Lock()
        # key: "host:port" -> {"active": bool, "last_heartbeat": float}
        self._workers = {}

    def upsert(self, info):
        key = f"{info[0]}:{info[1]}"
        with self._lock:
            now = time.time()
            self._workers[key] = {"active": True, "last_heartbeat": now}

    def mark_heartbeat(self, key: str):
        with self._lock:
            if key in self._workers:
                self._workers[key]["last_heartbeat"] = time.time()
                self._workers[key]["active"] = True

    def remove(self, key: str):
        with self._lock:
            self._workers.pop(key, None)

    def list_workers(self):
        with self._lock:
            return list(self._workers.keys())

    def list_active_workers(self, timeout_seconds: float = 15.0):
        cutoff = time.time() - timeout_seconds
        with self._lock:
            return [k for k, v in self._workers.items() if v["last_heartbeat"] >= cutoff]


STATE = _State()


def port_map(id: str):
    client = docker.from_env()
    c = client.containers.get(id)
    return c.attrs["NetworkSettings"]["Ports"]

def output_bindings(ports_dict):
    """Debugs output of port_map"""
    bindings = []
    for container_port, host_list in (ports_dict or {}).items():
        if host_list:
            for b in host_list:
                bindings.append(f'{b["HostIp"]}:{b["HostPort"]} -> {container_port}')
        else:
            bindings.append(f'(not published) -> {container_port}')
    return bindings


class RegistryServicer(worker_to_master_pb2_grpc.RegistryServicer):
    def Register(self, request: worker_to_master_pb2.WorkerInfo, context): 
        ports_dict = port_map(request.id)
        for val in ports_dict.values():
            port = val[0]["HostPort"]
        # LOG.info("\n".join(output_bindings(ports_dict)))

        STATE.upsert((request.id, port))
        LOG.info("Registered worker %s", request.id)
        
        # Log current worker pool
        workers = STATE.list_workers()
        LOG.info("Current worker pool: %s", 
                 "; ".join(w for w in workers))
        
        return worker_to_master_pb2.RegisterReply(ok=True, message="registered")

    def Heartbeat(self, request: worker_to_master_pb2.HeartbeatRequest, context):
        # The worker_id is the container id; map to a key by discovering its published port
        try:
            ports_dict = port_map(request.worker_id)
            for val in ports_dict.values():
                port = val[0]["HostPort"]
            key = f"{request.worker_id}:{port}"
            STATE.mark_heartbeat(key)
            return worker_to_master_pb2.HeartbeatReply(ok=True, message="pong")
        except Exception as e:
            LOG.warning("Heartbeat handling failed for %s: %s", request.worker_id, e)
            return worker_to_master_pb2.HeartbeatReply(ok=False, message=str(e))


def make_registry_server(bind):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    worker_to_master_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), server)
    server.add_insecure_port(bind)
    return server


def serve_both():
    master_srv = make_master_server('[::]:50051')
    registry_srv = make_registry_server('[::]:8081')

    master_srv.start()
    registry_srv.start()

    LOG.info("Starting master_client gRPC server on port 50051")
    LOG.info("Starting registry server on port 8081")

    # Background health monitor
    def health_monitor():
        timeout_s = 15.0
        while True:
            active = set(STATE.list_active_workers(timeout_s))
            all_workers = set(STATE.list_workers())
            dead = all_workers - active
            for key in dead:
                LOG.warning("Removing inactive worker: %s", key)
                STATE.remove(key)
            time.sleep(5)

    hm_thread = threading.Thread(target=health_monitor, daemon=True)
    hm_thread.start()

    # Wait on both; keep main thread alive
    try:
        t1 = threading.Thread(target=master_srv.wait_for_termination, daemon=True)
        t2 = threading.Thread(target=registry_srv.wait_for_termination, daemon=True)
        t1.start()
        t2.start()
        LOG.info("Master node is ready")
        while t1.is_alive() and t2.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        LOG.info("Shutting down gracefully...")
        master_srv.stop(grace=None)
        registry_srv.stop(grace=None)
        LOG.info("Shutdown complete")


if __name__ == '__main__':
    serve_both()
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

import master_to_worker_pb2
import master_to_worker_pb2_grpc

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

WORKER_PORT = 50051

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
            # TODO: Implement partitioning logic
            data_paths = request.file_paths
            map_fn = request.map
            reduce_fn = request.reduce
            job_path = request.job_path
            num_reducers = request.num_reducers
            req = master_to_worker_pb2.MapTaskRequest(
                job_id=1,
                job_path=job_path,
                function_name=map_fn,
                data_paths=data_paths,
                num_reducers=num_reducers,
                output_dir = f"{job_path}/intermediate",
                task_id=1
            )
            # For test, assign all map tasks to the first available worker
            STATE._lock.acquire()
            workers = STATE.list_active_workers()
            STATE._lock.release()

            if not workers:
                raise RuntimeError("No active workers available")
            first_worker = workers[0]
            with grpc.insecure_channel(f"{first_worker}:{WORKER_PORT}") as ch:
                stub = master_to_worker_pb2_grpc.WorkerTaskStub(ch)
                map_resp = stub.RunMap(req)
                if not map_resp.ok:
                    raise RuntimeError("Map task failed. Message: " + map_resp.message)

            return master_client_pb2.MapReduceResponse(
                ok=True,
                file_paths=[],
            )
        except Exception as e:
            LOG.error("MapReduce failed: %s", str(e))
            return master_client_pb2.MapReduceResponse(
                ok=False,
                file_paths=[]
            )



class _State:
    """In-memory worker registry."""
    def __init__(self):
        self._lock = threading.Lock()
        # key: worker_id -> {"active": bool, "last_heartbeat": float}
        self._workers = {}
        self._pending_requests = []
        self._tasks_left = {} #

    def upsert(self, worker_id: str):
        """Register or update a worker by its ID."""
        with self._lock:
            now = time.time()
            self._workers[worker_id] = {"active": True, "last_heartbeat": now}

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
        worker_id = request.id
        STATE.upsert(worker_id)
        LOG.info("Registered worker %s", worker_id)
        
        # Log current worker pool
        workers = STATE.list_workers()
        LOG.info("Current worker pool: %s", 
                 "; ".join(w for w in workers))
        
        return worker_to_master_pb2.RegisterReply(ok=True, message="registered")

    def Heartbeat(self, request: worker_to_master_pb2.HeartbeatRequest, context):
        """Process heartbeat from worker. Uses worker_id as key."""
        try:
            worker_id = request.worker_id
            STATE.mark_heartbeat(worker_id)
            LOG.debug("Heartbeat received from worker: %s", worker_id)
            return worker_to_master_pb2.HeartbeatReply(ok=True, message="pong")
        except Exception as e:
            LOG.warning("Heartbeat handling failed for %s: %s", request.worker_id, e)
            return worker_to_master_pb2.HeartbeatReply(ok=False, message=str(e))

def make_master_server(client_bind = '[::]:50051', worker_bind = '[::]:8081'):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_client_pb2_grpc.add_MasterClientServicer_to_server(MasterClientService(), server)
    worker_to_master_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), server)
    server.add_insecure_port(client_bind)
    server.add_insecure_port(worker_bind)
    return server

def serve_both():
    master_srv = make_master_server('[::]:50051', '[::]:8081')

    # Background health monitor
    def health_monitor():
        timeout_s = 15.0
        check_count = 0
        while True:
            active = set(STATE.list_active_workers(timeout_s))
            all_workers = set(STATE.list_workers())
            dead = all_workers - active
            
            # Report status periodically (every 12 checks = 1 minute)
            check_count += 1
            if check_count % 12 == 0:
                if active:
                    LOG.info("Health check: %d/%d workers active [%s]", 
                            len(active), len(all_workers), "; ".join(sorted(active)))
                elif all_workers:
                    LOG.warning("Health check: 0/%d workers active (all dead!)", len(all_workers))
            
            for key in dead:
                LOG.warning("Removing inactive worker: %s", key)
                STATE.remove(key)
            time.sleep(5)

    hm_thread = threading.Thread(target=health_monitor, daemon=True)
    hm_thread.start()

    master_srv.start()
    LOG.info("Starting master_client gRPC  on port 50051")
    LOG.info("Starting registry gRPC on port 8081")
    # Wait on both; keep main thread alive
    try:
        master_srv.wait_for_termination()
    except KeyboardInterrupt:
        LOG.info("Shutting down gracefully...")
        master_srv.stop(grace=None)
        LOG.info("Shutdown complete")


if __name__ == '__main__':
    serve_both()
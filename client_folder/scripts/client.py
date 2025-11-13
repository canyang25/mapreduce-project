import grpc
from pyarrow import fs
import os
from subprocess import check_output

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

import master_client_pb2
import master_client_pb2_grpc


os.environ["CLASSPATH"] = str(check_output([os.environ["HADOOP_HOME"]+"/bin/hdfs", "classpath", "--glob"]), "utf-8")

class Client:
    """This class abstracts MapReduce and HDFS file operations"""
    def __init__(self, namenode_host, namenode_hdfs_port, namenode_client_port):
        # Set up gRPC channel to master
        self.channel = grpc.insecure_channel(f'{namenode_host}:{namenode_client_port}')
        self.stub = master_client_pb2_grpc.MasterClientStub(self.channel)

        # Direct HDFS connection
        self.hdfs_client = fs.HadoopFileSystem(
            host=namenode_host,
            port=namenode_hdfs_port
        )
    
    def map_reduce(self, file_paths: list[str], map: str, reduce: str, job_path: str, num_reducers: int):
        """Call map reduce"""
        try:
            request = master_client_pb2.MapReduceRequest(
                file_paths=file_paths, 
                map=map, 
                reduce=reduce, 
                job_path=job_path, 
                num_reducers=num_reducers
            )
            
            response = self.stub.MapReduce(request)

            if response.success:
                return response
            else:
                raise Exception(f"Failed to read file via worker: {response.error_message}")
        except grpc.RpcError as e:
            raise Exception(f"gRPC error: {str(e)}")
    
    def read_file(self, file_path: str):
        """Read a file directly from HDFS"""
        try:
            with self.hdfs_client.open_input_file(file_path) as f:
                return f.readall().decode("utf-8")
        except Exception as e:
            raise Exception(f"Failed to read file directly: {str(e)}")
    
    def write_file(self, file_path: str, content: str):
        """Write a file directly to HDFS"""
        try:
            with self.hdfs_client.open_output_stream(file_path) as f:
                f.write(content)
        except Exception as e:
            raise Exception(f"Failed to write file directly: {str(e)}")

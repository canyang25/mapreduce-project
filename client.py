import grpc
import pyarrow as pa
import pyarrow.fs
import master_client_pb2
import master_client_pb2_grpc
import os
from subprocess import check_output

import time

os.environ["CLASSPATH"] = str(check_output([os.environ["HADOOP_HOME"]+"/bin/hdfs", "classpath", "--glob"]), "utf-8")

class Client:
    """Purpose of this class is to abstract MapReduce and HDFS file operations. Open to change the name"""
    def __init__(self, namenode_host, namenode_hdfs_port, namenode_client_port):
        # Set up gRPC channel to master
        self.channel = grpc.insecure_channel(f'{namenode_host}:{namenode_client_port}')
        self.stub = master_client_pb2_grpc.MasterClientStub(self.channel)

        # Direct HDFS connection
        self.hdfs_client = pa.fs.HadoopFileSystem(
            host=namenode_host,
            port=namenode_hdfs_port
        )
    
    def map_reduce(self, file_paths, map, reduce, num_reducers=1):
        """Read a file through the worker service"""
        try:
            request = master_client_pb2.MapReduceRequest(file_paths=file_paths, map=map, reduce=reduce, num_reducers=num_reducers)
            response = self.stub.MapReduce(request)
            if response.ok:
                return response
            else:
                raise Exception(f"Failed to read file via worker: {response.error_message}")
        except grpc.RpcError as e:
            raise Exception(f"gRPC error: {str(e)}")
    
    def read_file(self, file_path):
        """Read a file directly from HDFS"""
        try:
            with self.hdfs_client.open_input_file(file_path) as f:
                return f.readall()
        except Exception as e:
            raise Exception(f"Failed to read file directly: {str(e)}")
    
    def write_file(self, file_path, content):
        """Write a file directly to HDFS"""
        try:
            with self.hdfs_client.open_output_stream(file_path) as f:
                f.write(content)
        except Exception as e:
            raise Exception(f"Failed to write file directly: {str(e)}")

def main():
    # !!! The following is just to test client interations. We should change this to be more interactive !!!
    client = Client(
        namenode_host='boss',
        namenode_hdfs_port=9000,
        namenode_client_port=50051
    )

    # Needed to give time for HDFS to be ready
    time.sleep(5)

    # Test write/read to HDFS
    try:
        print("\nWriting file...")
        client.write_file("/test/hello.txt", b"Hello world!")
        
        print("Reading file...")
        content = client.read_file("/test/hello.txt")
        print(f"Read content: {content.decode()}")
        
    except Exception as e:
        print(f"Error using direct HDFS: {str(e)}")
    
    # Test MapReduce
    try:
        print("Scheduling MapReduce job...")
        response = client.map_reduce(["/test/hello.txt"], "map function", "reduce function")
        print(f"MapReduce response: success={response.ok}, job_id={response.job_id}")

    except Exception as e:
        print(f"Error using worker: {str(e)}")
    
    # Keep the client alive for inspection
    time.sleep(2000)
    

if __name__ == '__main__':
    main()
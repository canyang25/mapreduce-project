"""Unit tests for Client class. Targets client related tests"""

from client import Client

import unittest
import time
import importlib.util
import sys

def load_function_from_path(path, func_name):
    # Create module spec
    spec = importlib.util.spec_from_file_location("dynamic_module", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["dynamic_module"] = module
    spec.loader.exec_module(module)

    # Get the function
    return getattr(module, func_name)

class TestClient(unittest.TestCase):
    # Class setup
    @classmethod
    def setUpClass(cls):
        """Initialize client once for all tests."""
        cls.client = Client(
            namenode_host='boss',
            namenode_hdfs_port=9000,
            namenode_client_port=50051
        )
        # Give time for HDFS to be ready
        time.sleep(5)
    
    # Helper functions

    def run_job_locally(self, job_script: str, file_paths: list[str]):
        """Run a MapReduce job locally and sequencially using the provided map and reduce functions."""
        # Map phase
        map_function = load_function_from_path(job_script, "map_function")
        map_results = []
        
        for k1 in file_paths:
            v1 = self.client.read_file(k1)
            map_results.extend(map_function(k1, v1))
            
        # Simulate grouping by key
        from collections import defaultdict
        grouped = defaultdict(list)
        for k2, v2 in map_results:
            grouped[k2].append(v2)
        
        # Reduce phase
        reduce_function = load_function_from_path(job_script, "reduce_function")
        reduce_results = []

        for k2, v2 in grouped.items():
            result = reduce_function(k2, v2)
            reduce_results.append(result)

        return reduce_results
    
    def one_call_map_reduce_helper(self, local_job_script: str, uploaded_job_script: str, file_paths: list[str]):
        # Write the job file to HDFS
        with open(local_job_script, 'rb') as f:
            fcn_b = f.read()
        self.client.write_file(uploaded_job_script, fcn_b)

        response = self.client.map_reduce(file_paths, 'map_function', 'reduce_function', uploaded_job_script, 2)

        # Check success message
        self.assertTrue(response.success)

        # Check correctness of output files by simulating job locally
        local_results = self.run_job_locally(local_job_script, file_paths)

        results = []
        for each in response.file_paths:
            print(f"MapReduce output file: {each}")
            content = self.client.read_file(each)
            print(content)
            results.append(content)
        
        self.assertEqual(results, local_results)

    # Tests

    def test_write_and_read_file(self):
        """Test write/read to HDFS."""
        self.client.write_file("/test/hello.txt", b"Hello world!")
        content = self.client.read_file("/test/hello.txt")
        self.assertEqual(content, "Hello world!")

    def test_one_file_word_count(self):
        """Test word count with one input file. NOTE: assumes upload_data.py has been run."""
        local_job_script = 'client_folder/jobs/word_count.py'
        uploaded_job_script = '/jobs/word_count.py'
        file_paths = ['/client_folder/data/small/file1.txt']

        self.one_call_map_reduce_helper(local_job_script, uploaded_job_script, file_paths)  
    
    def test_one_file_inverted_index(self):
        """Test inverted index with one input file. NOTE: assumes upload_data.py has been run."""
        local_job_script = 'client_folder/jobs/inverted_index.py'
        uploaded_job_script = '/jobs/inverted_index.py'
        file_paths = ['/client_folder/data/small/file1.txt']

        self.one_call_map_reduce_helper(local_job_script, uploaded_job_script, file_paths)
    
    def test_mutliple_file_inverted_index(self):
        """Test inverted index with multiple input files. NOTE: assumes upload_data.py has been run."""
        local_job_script = 'client_folder/jobs/inverted_index.py'
        uploaded_job_script = '/jobs/inverted_index.py'
        file_paths = ['/client_folder/data/small/file1.txt', '/client_folder/data/small/file2.txt', '/client_folder/data/small/file3.txt']

        self.one_call_map_reduce_helper(local_job_script, uploaded_job_script, file_paths)
    
    def test_multiple_file_word_count(self):
        """Test multiple successive word count calls. NOTE: assumes upload_data.py has been run."""
        local_job_script = 'client_folder/jobs/word_count.py'
        uploaded_job_script = '/jobs/word_count.py'
        file_paths = ['/client_folder/data/small/file1.txt']

        # Write the job file to HDFS
        with open(local_job_script, 'rb') as f:
            fcn_b = f.read()
        self.client.write_file(uploaded_job_script, fcn_b)

        responses = []
        for i in range(3):
            file_paths = [f'/client_folder/data/small/file{ i + 1 }.txt']
            response = self.client.map_reduce(file_paths, 'map_function', 'reduce_function', uploaded_job_script, 2)
            responses.append(response)

        # Check success message
        self.assertTrue(response.success)

        # Check correctness of output files by simulating job locally
        local_results = self.run_job_locally(local_job_script, file_paths)

        results = []
        for each_response in responses:
            result = []
            for each in each_response.file_paths:
                print(f"MapReduce output file: {each}")
                content = self.client.read_file(each)
                print(content)
                result.append(content)
            results.append(result)
        
        for result in results:
            self.assertEqual(result, local_results)

if __name__ == '__main__':
    unittest.main()
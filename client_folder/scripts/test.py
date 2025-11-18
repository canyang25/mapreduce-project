"""Unit tests for Client class. Targets client related tests"""

from client import Client

import unittest
import time
import importlib.util
import sys
import ast

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
        self.maxDiff = None
    
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


    def _parse_mapreduce_output(self, content: str) -> list[tuple]:
        """Parse MapReduce output file content into list of (key, value) tuples.

        Expected file format: alternating lines of key and value, e.g.
            word1\ncount1\nword2\ncount2\n...
        Values may be integers or Python literals (lists). We try to
        deserialize using int() first, then ast.literal_eval, falling back
        to the raw string.
        """
        if isinstance(content, bytes):
            content = content.decode("utf-8")

        lines = [l for l in content.splitlines() if l != ""]
        pairs = []
        i = 0
        while i < len(lines):
            key = lines[i]
            val = None
            if i + 1 < len(lines):
                raw = lines[i + 1]
                # try int
                try:
                    val = int(raw)
                except Exception:
                    # try ast literal (e.g., list repr)
                    try:
                        val = ast.literal_eval(raw)
                    except Exception:
                        val = raw
            pairs.append((key, val))
            i += 2

        return pairs
    
    def one_call_map_reduce_helper(self, local_job_script: str, uploaded_job_script: str, file_paths: list[str]):
        # Write the job file to HDFS
        with open(local_job_script, 'rb') as f:
            fcn_b = f.read()
        self.client.write_file(uploaded_job_script, fcn_b)

        response = self.client.map_reduce(file_paths, 'map_function', 'reduce_function', uploaded_job_script, 2, 'iterator_fn')

        # Check success message
        self.assertTrue(response.ok)

        # Check correctness of output files by simulating job locally
        local_results = self.run_job_locally(local_job_script, file_paths)

        # Convert local results (list of tuples) into dict for order-insensitive comparison
        local_dict = {k: v for k, v in local_results}

        # Read and parse MapReduce output files into tuples, then dict
        remote_pairs = []
        for each in response.file_paths:
            print(f"MapReduce output file: {each}")
            content = self.client.read_file(each)
            print(content)
            parsed = self._parse_mapreduce_output(content)
            remote_pairs.extend(parsed)

        remote_dict = {k: v for k, v in remote_pairs}

        self.assertEqual(remote_dict, local_dict)

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
    
    def test_multiple_calls_word_count(self):
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
        for each_response in responses:
            self.assertTrue(each_response.ok)

        # Check correctness of output files by simulating job locally
        local_results = self.run_job_locally(local_job_script, file_paths)
        local_dict = {k: v for k, v in local_results}

        # For each response, build its dict and compare to local_dict
        for each_response in responses:
            remote_pairs = []
            for each in each_response.file_paths:
                print(f"MapReduce output file: {each}")
                content = self.client.read_file(each)
                print(content)
                parsed = self._parse_mapreduce_output(content)
                remote_pairs.extend(parsed)

            remote_dict = {k: v for k, v in remote_pairs}
            self.assertEqual(remote_dict, local_dict)

if __name__ == '__main__':
    unittest.main()
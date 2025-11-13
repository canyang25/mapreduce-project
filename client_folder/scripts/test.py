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
        
        for each in file_paths:
            in_text = self.client.read_file(each)
            map_results.extend(map_function(in_text))
            
        # Simulate grouping by key
        from collections import defaultdict
        grouped = defaultdict(list)
        for word, count in map_results:
            grouped[word].append(count)
        
        # Reduce phase
        reduce_function = load_function_from_path(job_script, "reduce_function")
        reduce_results = []

        for word, counts in grouped.items():
            result = reduce_function(word, counts)
            reduce_results.append(result)

        return reduce_results

    # Tests

    def test_write_and_read_file(self):
        """Test write/read to HDFS."""
        self.client.write_file("/test/hello.txt", b"Hello world!")
        content = self.client.read_file("/test/hello.txt")
        self.assertEqual(content, "Hello world!")

    def test_map_reduce(self):
        """Test MapReduce. NOTE: assumes upload_data.py has been run."""
        local_job_script = 'client_folder/jobs/word_count.py'
        uploaded_job_script = '/jobs/word_count.py'
        file_paths = ['/client_folder/data/small/file1.txt']

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

        


if __name__ == '__main__':
    unittest.main()
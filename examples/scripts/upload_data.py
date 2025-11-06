#!/usr/bin/env python3
"""
Upload sample data files to HDFS.

This script uploads all files from examples/data/ to HDFS directories,
making them available for MapReduce jobs.

Usage:
    # From the project root:
    python3 examples/scripts/upload_data.py
    
    # Or from within a Docker container:
    python3 /app/examples/scripts/upload_data.py
"""

import os
import sys
from pathlib import Path

# Add project root to path for importing Client
# Works both locally (from project root) and in Docker (/app)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Also try /app for Docker container context
if '/app' not in str(project_root):
    sys.path.insert(0, '/app')

from client import Client
import time


def upload_directory(client, local_dir, hdfs_base_path):
    """
    Upload all files from a local directory to HDFS.
    
    Args:
        client: Client instance with HDFS connection
        local_dir: Path to local directory containing files
        hdfs_base_path: Base HDFS path where files should be uploaded
    """
    local_path = Path(local_dir)
    
    if not local_path.exists():
        print(f"Warning: Directory {local_dir} does not exist")
        return
    
    txt_files = list(local_path.glob("*.txt"))
    
    if not txt_files:
        print(f"No .txt files found in {local_dir}")
        return
    
    print(f"\nUploading {len(txt_files)} file(s) from {local_dir} to {hdfs_base_path}...")
    
    for txt_file in sorted(txt_files):
        hdfs_path = f"{hdfs_base_path}/{txt_file.name}"
        
        try:
            with open(txt_file, 'rb') as f:
                content = f.read()
            
            client.write_file(hdfs_path, content)
            print(f"  ✓ Uploaded: {txt_file.name} -> {hdfs_path}")
            
        except Exception as e:
            print(f"  ✗ Failed to upload {txt_file.name}: {e}")


def main():
    """Main function to upload all example data to HDFS."""
    
    # Initialize client
    print("Connecting to HDFS...")
    client = Client(
        namenode_host='boss',
        namenode_hdfs_port=9000,
        namenode_client_port=50051
    )
    
    # Wait for HDFS to be ready (if needed)
    print("Waiting for HDFS to be ready...")
    time.sleep(3)
    
    # Get the script's directory and find examples/data
    # Try multiple possible locations (local dev vs Docker)
    script_dir = Path(__file__).parent
    possible_roots = [
        script_dir.parent.parent,  # Local: examples/scripts/ -> project root
        Path("/app"),              # Docker: /app/examples/scripts/
        Path.cwd(),                # Current working directory
    ]
    
    data_dir = None
    for root in possible_roots:
        candidate = root / "examples" / "data"
        if candidate.exists():
            data_dir = candidate
            break
    
    if data_dir is None:
        print("ERROR: Could not find examples/data directory")
        print(f"Tried: {[str(r / 'examples' / 'data') for r in possible_roots]}")
        return
    
    # Upload small dataset
    small_dir = data_dir / "small"
    if small_dir.exists():
        upload_directory(client, small_dir, "/examples/data/small")
    
    # Upload medium dataset
    medium_dir = data_dir / "medium"
    if medium_dir.exists():
        upload_directory(client, medium_dir, "/examples/data/medium")
    
    print("\n=== Upload Complete ===")
    print("\nYou can now use these files in MapReduce jobs:")
    print("  Small dataset:")
    print("    /examples/data/small/file1.txt")
    print("    /examples/data/small/file2.txt")
    print("    /examples/data/small/file3.txt")
    print("  Medium dataset:")
    print("    /examples/data/medium/large_file1.txt")
    print("    /examples/data/medium/large_file2.txt")
    
    # Verify by reading one file back
    print("\n=== Verification ===")
    try:
        test_content = client.read_file("/examples/data/small/file1.txt")
        print(f"✓ Successfully read back test file ({len(test_content)} bytes)")
    except Exception as e:
        print(f"⚠ Could not verify upload: {e}")


if __name__ == "__main__":
    main()

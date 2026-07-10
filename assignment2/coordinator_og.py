import rpyc
import string
import collections
import itertools
import time
import operator
import glob
import os
import sys
import requests
import zipfile
import threading
from contextlib import ExitStack

def mapreduce_wordcount(input_files):
    """
    Orchestrates a MapReduce job for word counting across multiple workers.
    Thread-safe implementation with timeout handling and failure recovery.
    """
    # Get configuration from environment variables or defaults
    try:
        NUM_MAP_WORKERS = int(os.getenv("NUM_MAP_WORKERS", 3))
        NUM_REDUCE_WORKERS = int(os.getenv("NUM_REDUCE_WORKERS", 3))
        TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", 20))
        REDUCE_TASK_TIMEOUT = int(os.getenv("REDUCE_TASK_TIMEOUT", 120))  # Longer timeout for reduce tasks
        # Generate worker hostnames based on docker-compose service names
        WORKERS = [(f"worker-{i+1}", 18861) for i in range(NUM_MAP_WORKERS)]
    except ValueError as e:
        print(f"Error: Invalid environment variable. {e}")
        sys.exit(1)

    print("Coordinator starting...")
    print(f" - {NUM_MAP_WORKERS} map tasks")
    print(f" - {NUM_REDUCE_WORKERS} reduce tasks")
    print(f" - Map task timeout: {TASK_TIMEOUT} seconds")
    print(f" - Reduce task timeout: {REDUCE_TASK_TIMEOUT} seconds")

    # Thread-safe state management
    map_lock = threading.Lock()
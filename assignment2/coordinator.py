import rpyc
import collections
import time
import operator
import glob
import os
import sys
import requests
import zipfile
import threading
import socket

def mapreduce_wordcount(input_files):
    """
    Orchestrates a MapReduce job for word counting across multiple workers.
    Thread-safe implementation with timeout handling and failure recovery.
    """
    # Get configuration from environment variables or use defaults.
    try:
        NUM_MAP_WORKERS = int(os.getenv("NUM_MAP_WORKERS", 3))
        NUM_REDUCE_WORKERS = int(os.getenv("NUM_REDUCE_WORKERS", 3))
        TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", 20))
        REDUCE_TASK_TIMEOUT = int(os.getenv("REDUCE_TASK_TIMEOUT", 120)) # Longer timeout for reduce tasks.
    except ValueError as e:
        print(f"Error: Invalid environment variable value. {e}")
        sys.exit(1)

    print("Coordinator starting...")
    print(f" - {NUM_MAP_WORKERS} map tasks")
    print(f" - {NUM_REDUCE_WORKERS} reduce tasks")

    # Discover worker IPs from the 'worker' service hostname.
    WORKERS = []
    try:
        worker_ips = socket.gethostbyname_ex('worker')[2]
        # Ensure we don't use more workers than specified.
        for i in range(min(NUM_MAP_WORKERS, len(worker_ips))):
            WORKERS.append((worker_ips[i], 18861))
    except socket.gaierror:
        print("Could not resolve worker hostname.")
        WORKERS = [('localhost', 18861) for _ in range(NUM_MAP_WORKERS)]

    # Locks for queue and the map and reduce phases.    
    queue_lock = threading.Lock()
    map_lock = threading.Lock()
    reduce_lock = threading.Lock()

    # Split input into text chunks.
    map_tasks = create_map_tasks(input_files, NUM_MAP_WORKERS)
    if not map_tasks:
        print("No text to process.")
        return []
    print(f"Starting map phase with {len(map_tasks)} tasks.")
    
    # Thread-safe connection pool and task management
    worker_connections = []
    print("Connecting to workers.")
    for host, port in WORKERS:
        max_retries = 5
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                # Allow pickle protocol and better serialization.
                from rpyc.core.protocol import DEFAULT_CONFIG
                config = DEFAULT_CONFIG.copy()
                config.update({
                    "sync_request_timeout": TASK_TIMEOUT,
                    "allow_pickle": True,
                    "allow_public_attrs": True,
                    "allow_all_attrs": True,
                })
                connection = rpyc.connect(host, port, config=config)
                worker_connections.append(connection)
                print(f"Connected to {host}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Connection attempt {attempt + 1} failed for {host}:{port} - {e}, retrying.")
                    time.sleep(retry_delay)
                else:
                    print(f"Error connecting to {host}:{port} after {max_retries} attempts: {e}.")
                    sys.exit(1)

    pending_tasks = {}
    # Create task queue - store only indices to avoid keeping chunks in memory (task_id, (filepath, start, end)).
    task_queue = list(enumerate(map_tasks))
    map_results_by_id = {}
    import gc

    while len(map_results_by_id) < len(map_tasks):
        # Assign tasks to available workers in a thread-safe manner.
        with queue_lock:
            available_workers = [i for i in range(len(WORKERS)) if i not in pending_tasks]
            for worker_index in available_workers:
                if task_queue:
                    task_id, task_info = task_queue.pop(0)
                    try:
                        # Send the task definition (filepath, start, end) to the worker.
                        async_map = rpyc.async_(worker_connections[worker_index].root.map)
                        future = async_map(task_info)
                        pending_tasks[worker_index] = (task_id, task_info, future, time.time())
                    except Exception as e:
                        # Re-queue task on error.
                        print(f"Error assigning map task {task_id} to worker {worker_index+1}: {e}.")
                        task_queue.append((task_id, task_info))

        # Check for completed or timed-out tasks in a thread_safe manner.
        with map_lock:
            completed_workers = []
            for worker_index, task_info in list(pending_tasks.items()):
                task_id, original_task_info, future, start_time = task_info
                if future.ready:
                    try:
                        result = future.value
                        try:
                            # We're using pickle for efficient memory usage.
                            import pickle
                            result = pickle.loads(pickle.dumps(result))
                        except Exception as e:
                            print(f"Pickle conversion failed: {e}.")
                        map_results_by_id[task_id] = result
                    except Exception as e:
                        print(f"Map task {task_id} on worker {worker_index+1} failed: {e}. Re-queuing.")
                        with queue_lock:
                            task_queue.append((task_id, original_task_info))
                        completed_workers.append(worker_index)
                # If we hit the timeout re-queue.
                elif time.time() - start_time > TASK_TIMEOUT:
                    print(f"Map task {task_id} on worker {worker_index+1} timed out. Re-queuing.")
                    with queue_lock:
                        task_queue.append((task_id, original_task_info))
                    completed_workers.append(worker_index)
        
        # Clean up pending tasks.
        for worker_index in completed_workers:
            with queue_lock:
                if worker_index in pending_tasks:
                    del pending_tasks[worker_index]

        time.sleep(0.5)
    
    # Shuffle phase.
    print("Starting shuffle phase.")
    # Create the reduce task data directly, processing one map result at a time for memory efficiency.
    reduce_tasks_data = [collections.defaultdict(list) for _ in range(NUM_REDUCE_WORKERS)]
    for map_task_id in sorted(map_results_by_id.keys()):
        map_result = map_results_by_id.pop(map_task_id) 
        partitions_from_mapper = partition_dict(map_result, NUM_REDUCE_WORKERS)
        for i in range(NUM_REDUCE_WORKERS):
            for word, count in partitions_from_mapper[i].items():
                reduce_tasks_data[i][word].append(count)
        
        # Explicitly free memory.
        map_result = None
        partitions_from_mapper = None
        gc.collect()

    # Reduce phase.
    print("Starting reduce phase.")
    sys.stdout.flush()
    reduced_results = []
    
    pending_tasks = {}
    task_queue = list(enumerate(reduce_tasks_data))
    reduce_results_by_id = {}

    while len(reduce_results_by_id) < len(reduce_tasks_data):
        # Assign tasks to available workers in a thread-safe manner.
        with queue_lock:
            available_workers = [i for i in range(len(WORKERS)) if i not in pending_tasks]
            for worker_index in available_workers:
                if task_queue:
                    task_id, data = task_queue.pop(0)
                    try:
                        async_reduce = rpyc.async_(worker_connections[worker_index].root.reduce)
                        future = async_reduce(data)
                        pending_tasks[worker_index] = (task_id, future, time.time())
                    except Exception as e:
                        print(f"Error assigning reduce task {task_id} to worker {worker_index+1}: {e}.")
                        task_queue.append((task_id, data))

        # Check for completed or timed-out tasks in a thread_safe manner.
        with reduce_lock:
            completed_workers = []
            for worker_index, (task_id, future, start_time) in list(pending_tasks.items()):
                if future.ready:
                    try:
                        result = future.value
                        try:
                            # We're using pickle for efficient memory usage.
                            import pickle
                            result = pickle.loads(pickle.dumps(result))
                        except Exception as e:
                            print(f"Pickle conversion failed: {e}.")
                        reduce_results_by_id[task_id] = result
                        completed_workers.append(worker_index)
                    except Exception as e:
                        print(f"Reduce task {task_id} on worker {worker_index+1} failed: {e}. Re-queuing.")
                        with queue_lock:
                            task_queue.append((task_id, reduce_tasks_data[task_id]))
                        completed_workers.append(worker_index)
                # If we hit the timeout re-queue.
                elif time.time() - start_time > REDUCE_TASK_TIMEOUT:
                    print(f"Reduce task {task_id} on worker {worker_index+1} timed out. Re-queuing.")
                    with queue_lock:
                        task_queue.append((task_id, reduce_tasks_data[task_id]))
                    completed_workers.append(worker_index)

        # Clean up pending tasks.
        for worker_index in completed_workers:
            with queue_lock:
                if worker_index in pending_tasks:
                    del pending_tasks[worker_index]
        
        time.sleep(0.5)
    
    print(f"\nAll {len(reduce_results_by_id)} reduce tasks completed.")
    reduced_results = [reduce_results_by_id[i] for i in sorted(reduce_results_by_id.keys())]

    # Close all connections.
    for conn in worker_connections:
        try:
            conn.close()
        except:
            pass

    # Aggregation phase.
    print("Aggregating results.")
    total_counts = collections.Counter()
    for res in reduced_results:
        total_counts.update(res)
        
    return sorted(total_counts.items(), key=operator.itemgetter(1), reverse=True)

def create_map_tasks(input_files, num_workers):
    """
    Creates tasks for map workers by dividing files into byte ranges.
    Don't copy into memory to save space.
    """
    # Calculate total size.
    total_size = 0
    file_sizes = []
    for file in input_files:
        try:
            size = os.path.getsize(file)
            file_sizes.append((file, size))
            total_size += size
        except (FileNotFoundError, OSError) as e:
            print(f"Warning: Could not get size for {file}: {e}")
            continue
    
    if total_size == 0:
        print("No files found or all files are empty.")
        return []
        
    # Calculate target size per worker by distributing equally.
    target_chunk_size = total_size // num_workers
    tasks = []
    current_offset = 0

    # Split up the file.
    for file_path, file_size in file_sizes:
        with open(file_path, 'rb') as f:
            while current_offset < file_size:
                start = current_offset
                end = min(start + target_chunk_size, file_size)

                # Find the next newline to avoid splitting words, but only if not at the end.
                if end < file_size:
                    f.seek(end)
                    line = f.readline()
                    # If readline() returns empty, we're done.
                    if line:
                        end += len(line) - 1

                tasks.append((file_path, start, end))
                current_offset = end

                # Assign all remaining text to last worker.
                if len(tasks) == num_workers:
                    tasks[-1] = (file_path, tasks[-1][1], file_size)
                    current_offset = file_size

    print(f"Created {len(tasks)} map tasks from input files.")
    return tasks

def partition_dict(d, n): 
    """
    Partition a dictionary into n partitions based on key hash.
    """
    partitions = [{} for _ in range(n)]
    processed = 0
    for key, value in d.items():
        partition_index = hash(key) % n
        partitions[partition_index][key] = value
        processed += 1
    return partitions

def download(url_arg=None):
    """
    Downloads and unzips a wikipedia dataset in /txt/.
    """
    # Get URL from environment variable, command line args, or use default
    default_url = "https://mattmahoney.net/dc/enwik8.zip"
    
    # Handle if a list of args is passed from sys.argv.
    if isinstance(url_arg, list) and url_arg:
        url = url_arg[0]
    elif isinstance(url_arg, str):
        url = url_arg
    else:
        url = os.getenv("DATASET_URL", default_url)

    if not url:
        print("No dataset URL specified.")
        sys.exit(1)

    zip_file_name = os.path.basename(url)
    txt_dir = "txt"
    
    # Create the txt directory if it doesn't exist.
    os.makedirs(txt_dir, exist_ok=True)

    zip_file_path = os.path.join(os.getcwd(), zip_file_name)

    # Check if the zip file already exists.
    if not os.path.exists(zip_file_path):
        print(f"Downloading dataset from {url}...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(zip_file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download complete.")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading file: {e}")
            sys.exit(1)
    else:
        print(f"Dataset '{zip_file_name}' already exists. Skipping download.")

    try:
        # Check if already unzipped, if not, unzip.
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            unzipped_files = zip_ref.namelist()
            if os.path.exists(txt_dir) and any(os.path.exists(os.path.join(txt_dir, fname)) for fname in unzipped_files):
                print(f"Dataset already unzipped in '{txt_dir}/'. Skipping unzip.")
            else:
                print(f"Unzipping '{zip_file_name}' into '{txt_dir}/'...")
                zip_ref.extractall(txt_dir)
    except (zipfile.BadZipFile, IndexError) as e:
        print(f"Error unzipping file: {e}. It might be corrupted or not a zip file.")
    except Exception as e:
        print(f"An unexpected error occurred during unzipping: {e}")

if __name__ == "__main__":
    try:
        # Force immediate output
        print("Coordinator script starting.")
        
        # Get URL from command line args (if provided).
        url_arg = sys.argv[1:] if len(sys.argv) > 1 else None

        # Download and unzip dataset.
        download(url_arg)

        all_files = glob.glob('txt/*')
        input_files = [f for f in all_files if 'enwik9' in f and os.path.getsize(f) > 0]
        
        if not input_files:
            print("ERROR: No enwik8 input file found in the 'txt' directory.")
            print("Available files:", all_files)
            print("Make sure the dataset was downloaded and unzipped correctly.")
            sys.exit(1)

        print(f"Processing {len(input_files)} input file(s): {[os.path.basename(f) for f in input_files]}")

        # Start timer and run the mapreduce function.
        start_time = time.time()
        word_counts = mapreduce_wordcount(input_files)
        
        if word_counts:
            print('\n--- TOP 20 WORDS BY FREQUENCY ---\n')
            top20 = word_counts[0:20]
            try:
                longest = max(len(word) for word, count in top20)
                i = 1
                for word, count in top20:
                    print('%2d. %-*s: %s' % (i, longest+1, word, count))
                    i = i + 1
            except ValueError:
                print("Could not determine top 20 words.")
        else:
            print("Error: No word counts generated.")

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("\nTotal Elapsed Time: {:.2f} seconds".format(elapsed_time))
    
    except Exception as e:
        print(f"Error running the script: {e}.")
        sys.exit(1)
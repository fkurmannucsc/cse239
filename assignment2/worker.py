import rpyc
import string

class MapReduceService(rpyc.Service):
    def exposed_map(self, task_info):
        """
        Map step: tokenize and count words in text chunk.
        Receives a task_info tuple: (filepath, start_byte, end_byte)
        """
        filepath, start, end = task_info
        print(f"Worker received map task: {filepath} from byte {start} to {end}.")

        word_counts = {}
        import re

        try:
            # Read the file and tokenize the given range.
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(start)
                # Process the chunk line-by-line for memory optimization.
                while f.tell() < end:
                    line = f.readline()
                    if not line:
                        break
                    
                    # Tokenize and count words in the current line.
                    words = re.findall(r'\b\w+\b', line.lower())
                    for word in words:
                        word_counts[word] = word_counts.get(word, 0) + 1

        except Exception as e:
            print(f"Error reading file chunk for task {task_info}: {e}.")
            return {}
        
        return word_counts
    
    def exposed_reduce(self, grouped_words):
        """
        Reduce step: sum counts for a subset of words.        
        """
        try:
            print(f"Worker received reduce task.")            
            try:
                import pickle
                grouped_words = pickle.loads(pickle.dumps(grouped_words))                
            except Exception as e:
                print(f"Pickle conversion failed: {e}.")
            
            reduced_counts = {}
            for word, counts in grouped_words.items():
                reduced_counts[word] = sum(counts)
            
            print(f"Reduce task complete: {len(reduced_counts)} words processed.")
            return reduced_counts
        except Exception as e:
            print(f"Error in reduce: {e}.")
            raise
    
if __name__ == "__main__":
    import sys
    import os
    from rpyc.utils.server import ThreadedServer
    
    worker_name = os.getenv("HOSTNAME", "unknown")
    print(f"Starting MapReduce worker server: {worker_name}.")
    
    try:
        # Allow pickling for better serialization.
        from rpyc.core.protocol import DEFAULT_CONFIG
        config = DEFAULT_CONFIG.copy()
        config.update({
            "allow_pickle": True,
            "allow_public_attrs": True,
            "allow_all_attrs": True,
        })
        t = ThreadedServer(MapReduceService, port=18861, logger=None, protocol_config=config)
        print("Worker server started. Waiting for tasks.")
        t.start()
    except Exception as e:
        print(f"Error starting worker: {e}.")
        sys.exit(1)

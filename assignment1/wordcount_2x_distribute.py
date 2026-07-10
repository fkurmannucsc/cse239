import string
import collections
import itertools
import time
import operator
import glob

from contextlib import ExitStack
import multiprocessing
import os

class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func):
        """
        map_func
        Function to map inputs to intermediate data. Takes as
        argument one input value and returns a tuple with the key
        and a value to be reduced.
        
        reduce_func
        Function to reduce partitioned version of intermediate data
        to final output. Takes as argument a key as produced by
        map_func and a sequence of the values associated with that
        key.
        """
        self.map_func = map_func
        self.reduce_func = reduce_func

    def partition(self, mapped_values):
        """
        Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs):
        """
        Process the inputs through the map and reduce functions given.
        inputs
        An iterable containing the input data to be processed.
        """
        num_cores = os.cpu_count()  # Get the number of available CPU cores
        print(f"Distributing map step across {num_cores} cores...")
        
        # Parallelize the map step, which is I/O and CPU-bound
        with multiprocessing.Pool(processes=num_cores) as pool:
            map_responses = pool.map(self.map_func, inputs)
        
        # Sequential partition
        # The partition and reduce steps are fast, so they can be sequential
        partitioned_data = self.partition(itertools.chain(*map_responses))

        # Sequential reduce
        reduced_values = map(self.reduce_func, partitioned_data)

        # Parallel reduce
        # with multiprocessing.Pool(processes=num_cores) as pool:
        #     reduced_values = pool.map(self.reduce_func, partitioned_data)

        return reduced_values

def text_to_words(text_chunk):
    """
    Read a chunk of text and return a sequence of (word, occurances) values.
    """
    STOP_WORDS = set([
    'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
    'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])
    TR = "".maketrans(string.punctuation, ' ' * len(string.punctuation))
    
    print(f"Processing chunk in process {os.getpid()}")
    counts = collections.Counter()
    for word in text_chunk.translate(TR).lower().split():
        if word.isalpha() and word not in STOP_WORDS:
            counts[word] += 1
    return list(counts.items())

def count_words(item):
    """
    Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    word, occurances = item
    return (word, sum(occurances))

def read_chunks(f, num_lines=2000):
    """Generator to read a file in chunks of lines."""
    while True:
        lines = list(itertools.islice(f, num_lines))
        if not lines:
            break
        yield "".join(lines)

if __name__ == '__main__':
    start_time = time.time()
    input_files = glob.glob('txt/*')

    mapper = SimpleMapReduce(text_to_words, count_words)
    
    # Combine all files into a single stream for chunking
    with ExitStack() as stack:
        files = [stack.enter_context(open(fname, 'rt', errors='replace')) for fname in input_files]
        chunks = read_chunks(itertools.chain(*files))
        word_counts = list(mapper(chunks))

    word_counts = sorted(word_counts, key=operator.itemgetter(1))
    word_counts.reverse()

    print('\nTOP 20 WORDS BY FREQUENCY\n')
    top20 = word_counts[0:20]

    longest = max(len(word) for word, count in top20)
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(elapsed_time))

# Using the combiner and using the proper technique to distribute work to multiple cores, the execution time is 27 seconds. Problem is that the reduce step is sent out seperately instead of done by the mapping cores.
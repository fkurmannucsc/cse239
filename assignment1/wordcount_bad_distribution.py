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
        print("Number of cores: ", num_cores)

        map_responses = map(self.map_func, inputs)
        print(f"Distributing map step across {num_cores} cores...")
        
        # Parallelize the map step, which is I/O and CPU-bound
        with multiprocessing.Pool(processes=num_cores) as pool:
            map_responses = pool.map(self.map_func, inputs)
        
        # Sequential partition
        # The partition and reduce steps are fast, so they can be sequential
        partitioned_data = self.partition(itertools.chain(*map_responses))

        # Old code
        # reduced_values = map(self.reduce_func, partitioned_data)
        # Sequential reduce
        reduced_values = map(self.reduce_func, partitioned_data)

        # New code
        with multiprocessing.Pool(processes=num_cores) as pool:
            reduced_values = pool.map(self.reduce_func, partitioned_data)
        # Parallel reduce
        # with multiprocessing.Pool(processes=num_cores) as pool:
        #     reduced_values = pool.map(self.reduce_func, partitioned_data)

        return reduced_values

def file_to_words(filename):
    """
    Read a file and return a sequence of (word, occurances) values.
    """
    STOP_WORDS = set([
    'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
    'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])
    TR = "".maketrans(string.punctuation, ' ' * len(string.punctuation))
    
    print('reading', filename)
    output = []

    print(f"Processing {filename} in process {os.getpid()}")
    with open(filename, 'rt', errors='replace') as f:
        for line in f:
            if line.lstrip().startswith('..'): # Skip rst comment lines
                continue
            line = line.translate(TR) # Strip punctuation
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS:
                    output.append( (word, 1) )
    return output

def count_words(item):
    """
    Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    word, occurances = item
    return (word, sum(occurances))

if __name__ == '__main__':
    start_time = time.time()
    input_files = glob.glob('txt/*')
    print("Input files: ", input_files)

    mapper = SimpleMapReduce(file_to_words, count_words)
    word_counts = mapper(input_files)
    #print(list(word_counts))
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

    # Not using the combiner method and distributing work to multiple cores actually takes longer, 190 seconds, that said, in this case, the mapping is not properly going to multiple processes yet.
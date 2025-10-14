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
    def __init__(self, map_func):
        """
        map_func
        A function that takes an input chunk and returns a
        collections.Counter object with the word counts for that chunk.
        """
        self.map_func = map_func

    def __call__(self, inputs):
        """
        Process the inputs through the map and reduce functions given.
        inputs
        An iterable containing the input data to be processed.
        """
        # Get the number of available CPU cores.
        num_cores = os.cpu_count()
        print(f"Distributing map step across {num_cores} cores...")
        
        # Create a pool of workers.
        final_results = collections.Counter()
        with multiprocessing.Pool(processes=num_cores) as pool:
            # Get results as they are completed, allow the main process to start the final reduce immediately.
            for counter in pool.imap_unordered(self.map_func, inputs):
                final_results.update(counter)
        
        return final_results.items()

def text_to_words(text_chunk):
    """
    Read a chunk of text and return a sequence of (word, occurances) values.
    """
    STOP_WORDS = set([
    'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
    'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])
    TR = "".maketrans(string.punctuation, ' ' * len(string.punctuation))
    
    counts = collections.Counter()
    # Count instances of each word for a text chunk. Combine results into a condensed dictionary in the sub-process already.
    for word in text_chunk.translate(TR).lower().split():
        if word.isalpha() and word not in STOP_WORDS:
            counts[word] += 1
    return counts

def read_chunks(f, num_lines=20000):
    """
    Read a file in chunks of lines.
    """
    while True:
        lines = list(itertools.islice(f, num_lines))
        if not lines:
            break
        yield "".join(lines)

if __name__ == '__main__':
    start_time = time.time()
    input_files = glob.glob('txt/*')

    mapper = SimpleMapReduce(text_to_words)
    
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

# Using the combiner and using the imap_unordered method to distribute work to multiple cores only once, the execution time on my 16 core machine is 13 seconds.
import rpyc

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """Map step: tokenize and count words in text chunk."""
    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MapReduceService, port=18861)
    t.start()
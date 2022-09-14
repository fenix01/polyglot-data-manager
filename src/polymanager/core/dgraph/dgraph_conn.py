import pydgraph
import logging

class DGraphConn:
    # This as possible module to translate need to dgraph
    # language. Must hold as less as possible any check.

    def __init__(self, addresses=[], port=9080):
        # address is the ip or the dns of the database instance
        # port is the tcp port of the communication
        self.client_stubs = list()
        for address in addresses:
            server_addr = "{}:{}".format(address, port)
            client_stub = pydgraph.DgraphClientStub(server_addr)
            self.client_stubs.append(client_stub)
        self.client = pydgraph.DgraphClient(*self.client_stubs)
        self._log = logging.getLogger(__name__)

    def get_client(self):
        return self.client

    def close(self):
        for client_stub in self.client_stubs:
            client_stub.close()
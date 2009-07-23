from lazyboy.connection import *
import unittest
import time

class TestClient(unittest.TestCase):
    def test_client(self):
        client = cassandra.get_pool("votes")
        self.assert_(type(client) is cassandra.Client)
        
        try:
            client = cassandra.Client()
        except Exception, e:
            py.test.raises(TypeError, e)
        
    def test_InvalidGetSliceNoTable(self):
        client = cassandra.get_pool("votes")
        key = "1"
        table = "users"
        column = "test"
        start = -1
        end = -1
        try:
            client.get_slice(table, key, column, start, end)
        except cassandra.ErrorInvalidRequest, e:
            self.assert_(True == True)
                
    def test_InvalidClient(self):
        try:
            client = cassandra.get_pool("votegfdgdfgdfgdfgs")
        except cassandra.ErrorCassandraClientNotFound, e:
            self.assert_(True == True)
            
    def test_InsertBatchSuperColumnFamily(self):
        client = cassandra.get_pool("votes")
        timestamp = time.time()
        vote_id = "12345"
        url = "http://google.com/"
        votes = []
        columns = [cassandra.superColumn_t(vote_id, [cassandra.column_t(columnName = "456", value = "fake values", timestamp = timestamp)])]
        
        cfmap = {"votes": columns}

        row = cassandra.batch_mutation_super_t(table = "URI", key = vote_id, cfmap = cfmap)
        
        self.assert_(client.batch_insert_superColumn(row, 0) == None)
            
    def test_GetSliceSuperColumn(self):
        client = cassandra.get_pool("votes")
        key = "12345"
        table = "URI"
        column = "votes"
        start = -1
        end = -1
        results = client.get_slice_super(table, key, column, start, end)

        self.assert_(results[0].name == "12345")


if __name__ == '__main__':
    unittest.main()

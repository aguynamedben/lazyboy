from prophecy.connection import *
import py, time

class TestClient(object):
    def __init__(self):
        self.config = config.factory("cassandra")
    
    def testClient(self):
        client = cassandra.getClient("votes")
        assert type(client) is cassandra.Client
        
        try:
            client = cassandra.Client()
        except Exception, e:
            py.test.raises(TypeError, e)
        
    def testInvalidGetSliceNoTable(self):
        client = cassandra.getClient("votes")
        key = "1"
        table = "users"
        column = "test"
        start = -1
        end = -1
        try:
            client.get_slice(table, key, column, start, end)
        except cassandra.ErrorInvalidRequest, e:
            assert True == True
                
    def testInvalidClient(self):
        try:
            client = cassandra.getClient("votegfdgdfgdfgdfgs")
        except cassandra.ErrorCassandraClientNotFound, e:
            assert True == True
            
    def testInsertBatchSuperColumnFamily(self):
        client = cassandra.getClient("votes")
        timestamp = time.time()
        vote_id = "12345"
        url = "http://google.com/"
        votes = []
        columns = [cassandra.superColumn_t(vote_id, [cassandra.column_t(columnName = "456", value = "fake values", timestamp = timestamp)])]
        
        cfmap = {"votes": columns}

        row = cassandra.batch_mutation_super_t(table = "URI", key = vote_id, cfmap = cfmap)
        
        assert client.batch_insert_superColumn(row, 0) == None
            
    def testGetSliceSuperColumn(self):
        client = cassandra.getClient("votes")
        key = "12345"
        table = "URI"
        column = "votes"
        start = -1
        end = -1
        results = client.get_slice_super(table, key, column, start, end)

        assert results[0].name == "12345"

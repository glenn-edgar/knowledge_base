import datetime
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from kb_query_support import KB_Search
from kb_status_data import KB_Status_Data
from kb_job_table import KB_Job_Queue
from kb_stream import KB_Stream
from kb_rpc_client import KB_RPC_Client
from kb_rpc_server import KB_RPC_Server

class KB_Data_Structures:
    """
    A class to handle the data structures for the knowledge base.
    """
    def __init__(self, host, port, dbname, user, password):
        self.query_support = KB_Search(host, port, dbname, user, password)
        self.clear_filters = self.query_support.clear_filters
        self.search_label = self.query_support.search_label
        self.search_name = self.query_support.search_name
        self.search_property_key = self.query_support.search_property_key
        self.search_property_value = self.query_support.search_property_value
        self.search_path = self.query_support.search_path
        self.find_description = self.query_support.find_description
        self.find_description_paths = self.query_support.find_description_paths

        self.status_data = KB_Status_Data(self.query_support)
        self.find_status_node_ids = self.status_data.find_node_ids
        self.find_status_node_id = self.status_data.find_node_id
        self.get_status_data = self.status_data.get_status_data
        self.set_status_data = self.status_data.set_status_data
        self.find_status_table_keys = self.status_data.find_status_table_keys
        self.get_status_data = self.status_data.get_status_data
        
        self.job_queue = KB_Job_Queue(self.query_support)
        self.find_job_ids = self.job_queue.find_job_ids
        self.find_job_id = self.job_queue.find_job_id
        self.find_job_table_keys = self.job_queue.find_job_table_keys
        self.get_queued_number = self.job_queue.get_queued_number
        self.get_free_number = self.job_queue.get_free_number
        self.peak_job_data = self.job_queue.peak_job_data
        self.delete_job_data = self.job_queue.delete_job_data
        self.push_job_data = self.job_queue.push_job_data
        self.list_pending_jobs = self.job_queue.list_pending_jobs
        self.list_active_jobs = self.job_queue.list_active_jobs
        self.list_completed_jobs = self.job_queue.list_completed_jobs
        self.clear_job_queue = self.job_queue.clear_job_queue
        
        self.stream = KB_Stream(self.query_support)
        self.find_stream_ids = self.stream.find_stream_ids
        self.find_stream_id = self.stream.find_stream_id
        self.find_stream_table_keys = self.stream.find_stream_table_keys
        self.push_stream_data = self.stream.push_stream_data
        self.list_stream_data = self.stream.list_stream_data
        
        self.rpc_client = KB_RPC_Client(self.query_support)
        self.rpc_client_id_find = self.rpc_client.find_rpc_client_id
        self.rpc_client_ids_find = self.rpc_client.find_rpc_client_ids
        self.rpc_client_keys_find = self.rpc_client.find_rpc_client_keys
        self.rpc_client_free_slots_find = self.rpc_client.find_free_slots
        self.rpc_client_queued_slots_find = self.rpc_client.find_queued_slots
        self.rpc_client_reply_data_peak = self.rpc_client.peak_reply_data
        self.rpc_client_data_release = self.rpc_client.release_rpc_data
        self.rpc_client_queue_clear = self.rpc_client.clear_reply_queue
        self.rpc_client_data_push = self.rpc_client.push_reply_data
        self.rpc_client_waiting_jobs_list = self.rpc_client.list_waiting_jobs
        
        self.rpc_server = KB_RPC_Server(self.query_support)
        self.rpc_server_id_find = self.rpc_server.find_rpc_server_id
        self.rpc_server_ids_find = self.rpc_server.find_rpc_server_ids
        self.rpc_server_table_keys_find = self.rpc_server.find_rpc_server_table_keys
        self.rpc_server_processing_jobs_list = self.rpc_server.list_processing_jobs
        self.rpc_server_failed_jobs_list = self.rpc_server.list_failed_jobs
        self.rpc_server_pending_jobs_count = self.rpc_server.count_pending_jobs
        self.rpc_server_processing_jobs_count = self.rpc_server.count_processing_jobs
        self.rpc_server_completed_jobs_count = self.rpc_server.count_completed_jobs
        self.rpc_server_failed_jobs_count = self.rpc_server.count_failed_jobs
        self.rpc_server_job_counts_get = self.rpc_server.get_job_counts
        self.rpc_server_queue_push = self.rpc_server.push_rpc_queue
        self.rpc_server_queue_peak = self.rpc_server.peak_server_queue
        self.rpc_server_job_completion_mark = self.rpc_server.mark_job_completion
        self.rpc_server_job_failure_mark = self.rpc_server.mark_job_failure

    def test_client_queue(self, client_path):
        """
        Test client queue operations in a specific sequence.
        
        Args:
            client_path (str): The path to the client to test
        """
        # Initial state
        print("=== Initial State ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        self.rpc_client_queue_clear(client_path)
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        # Push first set of reply data
        print("\n=== Pushing First Set of Reply Data ===")
        request_id1 = uuid.uuid4()
        self.rpc_client_data_push(
            client_path, 
            request_id1, 
            "xxx", 
            "Action1", 
            "xxx", 
            {"data1": "data1"}, 
            "fff"
        )
        print(f"Pushed reply data with request ID: {request_id1}")
        
        request_id2 = str(uuid.uuid4())
        self.rpc_client_data_push(
            client_path, 
            request_id2, 
            "xxx", 
            "Action2", 
            "yyy", 
            {"data2": "data2"}, 
            "fff"
        )
        print(f"Pushed reply data with request ID: {request_id2}")
        
        # After first push
        print("\n=== After First Push ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Peek and release first data
        print("\n=== Peek and Release First Data ===")
        peak_data = self.rpc_client_reply_data_peak(client_path)
        print(f"Peek data: {peak_data}")
        self.rpc_client_data_release(client_path, peak_data[0])
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")

        # Repeated check of queued slots and waiting jobs
        print("\n=== Additional Queue Check ===")
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}",waiting_jobs)
        
        # Peek and release second data
        print("\n=== Peek and Release Second Data ===")
        peak_data = self.rpc_client_reply_data_peak(client_path)
        print(f"Peek data: {peak_data}")
        print(f"Peek data[0]: {peak_data[0]}")
        self.rpc_client_data_release(client_path, peak_data[0])
      
        # After second release
        print("\n=== After Second Release ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        self.rpc_client_data_release(client_path, peak_data[0])
        
        # After second release
        print("\n=== After Second Release ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Push second set of reply data
        print("\n=== Pushing Second Set of Reply Data ===")
        request_id3 = str(uuid.uuid4())
        self.rpc_client_data_push(
            client_path, 
            request_id3, 
            "xxx", 
            "Action1", 
            "xxx", 
            {"data1": "data1"}, 
            "fff"
        )
        print(f"Pushed reply data with request ID: {request_id3}")
        
        request_id4 = str(uuid.uuid4())
        self.rpc_client_data_push(
            client_path, 
            request_id4, 
            "xxx", 
            "Action2", 
            "yyy", 
            {"data2": "data2"}, 
            "fff"
        )
        print(f"Pushed reply data with request ID: {request_id4}")
        
        # After second push
        print("\n=== After Second Push ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        # Clear reply queue
        print("\n=== Clearing Reply Queue ===")
        self.rpc_client_queue_clear(client_path)
        print(f"Cleared reply queue for client path: {client_path}")
        
        # Final state
        print("\n=== Final State After Clear ===")
        free_slots = self.rpc_client_free_slots_find(client_path)
        print(f"Number of free slots: {free_slots}")
        
        queued_slots = self.rpc_client_queued_slots_find(client_path)
        print(f"Number of queued slots: {queued_slots}")
        
        waiting_jobs = self.rpc_client_waiting_jobs_list(client_path)
        print(f"Waiting jobs: {waiting_jobs}")
        
        print("\n=== Test Complete ===")

# Example usage:
if __name__ == "__main__":
    # Create a new KB_Search instance
    kb_data_structures = KB_Data_Structures(
        dbname="knowledge_base",
        user="gedgar",
        password="ready2go",
        host="localhost",
        port="5432"
    )   
    
    """
    Status Data
    """
    #node_ids = kb_data_structures.find_status_node_ids(
    #      node_name='info2_status',
    #    properties={'prop3': 'val3'},
    #    node_path='header1_link.header1_name.KB_STATUS_FIELD.info2_status'
    #)
    node_ids = kb_data_structures.find_status_node_ids(None,None,None)
    print(node_ids)
    
    status_table_keys = kb_data_structures.find_status_table_keys(node_ids)
    print(status_table_keys)
    
    node_ids = kb_data_structures.find_status_node_id(
        node_name='info2_status',
        properties={'prop3': 'val3'},
        node_path='header1_link.header1_name.KB_STATUS_FIELD.info2_status'
    )
    print(node_ids)
    
    status_table_keys = kb_data_structures.find_status_table_keys(node_ids)
    print(status_table_keys)
    
    description = kb_data_structures.find_description(node_ids[0])
    print("description", description)
    
    data = kb_data_structures.get_status_data(status_table_keys[0])
    print("data", data)

    kb_data_structures.set_status_data(status_table_keys[0], {"prop1": "val1", "prop2": "val2"})
    data = kb_data_structures.get_status_data(status_table_keys[0])
    print("data", data)
    
    """
    Job Queue
    """
    print("***************************  job queue data ***************************")
    
    node_ids = kb_data_structures.find_job_ids(node_name=None, properties=None, node_path=None)
    print("node_ids", node_ids)
    
    job_table_keys = kb_data_structures.find_job_table_keys(node_ids)
    print("jobnode_ids", job_table_keys)
    
    job_key = job_table_keys[0]
    print("job_key", job_key)
    
    queued_number = kb_data_structures.get_queued_number(job_key)
    print("queued_number", queued_number)
    
    free_number = kb_data_structures.get_free_number(job_key)
    print("free_number", free_number)
    
    print("peak_job_data", kb_data_structures.peak_job_data(job_key))
    print("push_job_data")
    
    kb_data_structures.push_job_data(job_key, {"prop1": "val1", "prop2": "val2"})
    queued_number = kb_data_structures.get_queued_number(job_key)
    print("queued_number", queued_number)
    
    free_number = kb_data_structures.get_free_number(job_key)
    print("free_number", free_number)
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_key))
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_key))
    print("list_completed_jobs", kb_data_structures.list_completed_jobs(job_key))
    
    job_data = kb_data_structures.peak_job_data(job_key)
    job_id = job_data[0]
    print("job_id", job_id)
    print("job_data", job_data)
    
    free_number = kb_data_structures.get_free_number(job_key)
    print("free_number", free_number)
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_key))
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_key))
    
    kb_data_structures.delete_job_data(job_id)
    free_number = kb_data_structures.get_free_number(job_key)
    print("free_number", free_number)
    
    print("list_pending_jobs", kb_data_structures.list_pending_jobs(job_key))
    print("list_active_jobs", kb_data_structures.list_active_jobs(job_key))
    print("peak_job_data", kb_data_structures.peak_job_data(job_key))
    
    kb_data_structures.clear_job_queue(job_key)
    free_number = kb_data_structures.get_free_number(job_key)
    print("free_number", free_number)
    
    """
    Stream tables
    """
    print("***************************  stream data ***************************")
    
    node_ids = kb_data_structures.find_stream_ids(node_name="info1_status", properties=None, node_path=None)
    print("node_ids", node_ids)
    
    stream_table_keys = kb_data_structures.find_stream_table_keys(node_ids)
    print("stream_table_keys", stream_table_keys)
    
    descriptions = kb_data_structures.find_description_paths(stream_table_keys)
    print("descriptions", descriptions)
    
    kb_data_structures.push_stream_data(stream_table_keys[0], {"prop1": "val1", "prop2": "val2"})
    print("list_stream_data", kb_data_structures.list_stream_data(stream_table_keys[0]))
    
    past_timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15)
    before_timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=5)
    print("past_timestamp", past_timestamp)
    print("past data")
    print("list_stream_data", kb_data_structures.list_stream_data(stream_table_keys[0], recorded_after=past_timestamp, recorded_before=before_timestamp))
    
    """
    RPC Functions
    """
    print("***************************  RPC Functions ***************************")
    
    node_ids = kb_data_structures.rpc_client_ids_find(node_name=None, properties=None, node_path=None)
    print("rpc_client_node_ids", node_ids)
    
    node_ids = kb_data_structures.rpc_client_ids_find(node_name=None, properties=None, node_path=None)
    print("rpc_client_node_ids", node_ids)
    
    client_keys = kb_data_structures.rpc_client_keys_find(node_ids)
    print("client_keys", client_keys)
    
    client_descriptions = kb_data_structures.find_description_paths(client_keys)
    print("client_descriptions", client_descriptions)
    
    kb_data_structures.test_client_queue(client_keys[0])
    
    
    exit()
    
    node_ids = kb_data_structures.rpc_server_id_find(node_name=None, properties=None, node_path=None)
    print("rpc_server_node_ids", node_ids)   
    
    server_keys = kb_data_structures.rpc_server_table_keys_find(node_ids)
    print("server_keys", server_keys)    
    
    server_descriptions = kb_data_structures.find_description_paths(server_keys)
    print("server_descriptions", server_descriptions)    
    
    kb_data_structures.query_support.disconnect()
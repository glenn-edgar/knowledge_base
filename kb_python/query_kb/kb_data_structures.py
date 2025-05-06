import psycopg2
from psycopg2.extras import RealDictCursor
from kb_query_support import KB_Search
from kb_status_data import KB_Status_Data
from kb_job_table import KB_Job_Queue
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

    node_ids = kb_data_structures.find_status_node_ids(
        #node_name= "info1_status",
        node_name= 'info2_status',
        properties={'prop3': 'val3'},
        node_path='header1_link.header1_name.KB_STATUS_FIELD.info2_status'
    )
    print(node_ids)

    status_table_keys = kb_data_structures.find_status_table_keys(node_ids)
    print(status_table_keys )
    node_ids = kb_data_structures.find_status_node_id(
            #node_name= "info1_status",
            node_name= 'info2_status',
            properties={'prop3': 'val3'},
            node_path='header1_link.header1_name.KB_STATUS_FIELD.info2_status'
        )
    print(node_ids)
    status_table_keys = kb_data_structures.find_status_table_keys(node_ids)
    print(status_table_keys )
    
    description = kb_data_structures.find_description(node_ids[0])
    print("description",description)
    
    data = kb_data_structures.get_status_data(status_table_keys[0])
    print("data",data)

    kb_data_structures.set_status_data(status_table_keys[0], {"prop1": "val1", "prop2": "val2"})
    data = kb_data_structures.get_status_data(status_table_keys[0])
    print("data",data)
    
    """
    Job Queue
    """
    
    node_ids = kb_data_structures.find_job_ids(node_name = None, properties = None, node_path = None)
    print("jobnode_ids",node_ids)
    
    kb_data_structures.query_support.disconnect()
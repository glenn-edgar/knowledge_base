import json

class KB_Job_Queue:
    """
    A class to handle the status data for the knowledge base.
    """
    def __init__(self, kb_search):
        self.kb_search = kb_search
    
    def find_job_id(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path, and data.
        """
        print(node_name, properties, node_path)
        result = self.find_node_ids(node_name, properties, node_path)
        if len(result) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(result) > 1:
            raise ValueError(f"Multiple nodes found matching path parameters: {node_name}, {properties}, {node_path}")
        return result
    
    def find_job_ids(self, node_name, properties, node_path):
        """
        Find the node id for a given node name, properties, node path :
        """
        print(node_name, properties, node_path)
        self.kb_search.clear_filters()
        self.kb_search.search_label("KB_JOB_QUEUE")
        if node_name is not None:
            self.kb_search.search_name(node_name)
        if properties is not None:
            for key in properties:
                self.kb_search.search_property_value(key, properties[key])
        if node_path is not None:
            self.kb_search.search_path(node_path)
        node_ids = self.kb_search.execute_query()
        
        if node_ids is None:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        if len(node_ids) == 0:
            raise ValueError(f"No node found matching path parameters: {node_name}, {properties}, {node_path}")
        return node_ids
    
    def find_job_table_keys(self, key_data):
       
            
        return_values = []
        for key in key_data:
            
            return_values.append(key[5])
        return return_values
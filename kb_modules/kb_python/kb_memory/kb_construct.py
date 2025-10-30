from typing import Dict, List, Any, Optional, Union, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
import copy
import os
import yaml
from .kb_ltree_search import KB_Ltree_Search

class ConstructMemDB():
    def __init__(self):
        self.kb_dict = {}
        self.kb_path_valuess = {}
        self.working_kb = None
        self.data = {}
        
    
    
    def add_kb(self, kb_name, description=""):
        if kb_name in self.kb_dict:
            raise ValueError(f"Knowledge base {kb_name} already exists")
        self.kb_dict[kb_name] = [kb_name]
        self.kb_path_valuess[kb_name] = {}
        
      
    def select_kb(self, kb_name):
        if kb_name not in self.kb_dict:
            raise ValueError(f"Knowledge base {kb_name} does not exist")
        self.working_kb = kb_name
        
        
   
    def add_composite_node(self, link, node_name, node_data,description=""):
        """
        Adds a composite node to the knowledge base.

        Args:
            link: The link associated with the composite node.
            node_name: The name of the composit node.
            node_data: Data associated with the composite node. Must be a dictionary.  
            description: Description of the composite node.
        """
        
        
        if not isinstance(description, str):
            raise TypeError("description must be a string")
        if not isinstance(node_data, dict):
            raise TypeError("node_data must be a dictionary")
        
        if description != None:
            node_data["description"] = description
            


        self.kb_dict[self.working_kb].append(link)
        self.kb_dict[self.working_kb].append(node_name)
        node_path = ".".join(self.kb_dict[self.working_kb])
    
        if node_path in self.kb_path_valuess[self.working_kb]:
            raise ValueError(f"Path {node_path} already exists in knowledge base")
        
        self.kb_path_valuess[self.working_kb][node_path] = True
       
        path = ".".join(self.kb_dict[self.working_kb])
        self.store(path, node_data)
       

    def add_leaf_node(self, link, node_name,  node_data,description=""):
        self.add_composite_node(link, node_name,  node_data,description)
     
        self.kb_dict[self.working_kb].pop()  # Remove node_name
        self.kb_dict[self.working_kb].pop()  # Remove link
        
    
    def leave_composite_node(self, label, name ):
        """
        Leaves a composite node, verifying the label and name.
        If an error occurs, the knowledge_base table is deleted if it exists
        and the PostgreSQL connection is terminated.

        Args:
            label: The expected link of the composite node.
            name: The expected name of the composite node.
        """
        # Try to pop the expected values
        if not self.kb_dict[self.working_kb]:
            raise ValueError("Cannot leave a composite node: path is empty")
        
        ref_name = self.kb_dict[self.working_kb].pop()
        if not self.kb_dict[self.working_kb]:
            # Put the name back and raise an error
            self.kb_dict[self.working_kb].append(ref_name)
            raise ValueError("Cannot leave a composite node: not enough elements in path")
            
        ref_label = self.kb_dict[self.working_kb].pop()
        
        # Verify the popped values
        if ref_name != name or ref_label != label:
            # Create a descriptive error message
            error_msg = []
            if ref_name != name:
                error_msg.append(f"Expected name '{name}', but got '{ref_name}'")
            if ref_label != label:
                error_msg.append(f"Expected label '{label}', but got '{ref_label}'")
                
            raise AssertionError(", ".join(error_msg))
        
        
   
    
    def check_installation(self):
        """
        Checks if the installation is correct by verifying that the path is empty.
        If the path is not empty, the knowledge_base table is deleted if present,
        the database connection is closed, and an exception is raised.
        If the path is empty, the database connection is closed normally.

        Returns:
            bool: True if installation check passed

        Raises:
            RuntimeError: If the path is not empty
        """
        for kb_name in self.kb_dict:
            if len(self.kb_dict[kb_name]) != 1:
                
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.kb_dict[kb_name]}")
            if self.kb_dict[kb_name][0] != kb_name:
                raise RuntimeError(f"Installation check failed: Path is not empty for knowledge base {kb_name}. Path: {self.path[kb_name]}")
                
                
                
    def store(self, ltree_name: str, data: dict) -> bool:
        """
        Store data at a specific path in the tree.
        
        Args:
            path: The ltree path
            data: The data to store
           
            
        Returns:
            True if successful
        """
        
        if not KB_Ltree_Search.validate_path(ltree_name):
            raise ValueError(f"Invalid ltree path: {ltree_name}")
        temp_list = ltree_name.split(".")
        label_name = temp_list[-2]
        node_name = temp_list[-1]
        self.data[ltree_name] = {
            'ltree_name': ltree_name,
            'data': copy.deepcopy(data),
            'volatile_data': {},  # created for runtime r/w
        }
           
        return True
    
    def export_to_yaml(self, file_name):
        with open(file_name, 'w', encoding='utf-8') as file:  # 'utf-8' for non-ASCII support
            yaml.safe_dump(self.data, file, sort_keys=False, default_flow_style=False, allow_unicode=True)
                
                
if __name__ == '__main__':
    
    
    kb = ConstructMemDB()
    
    kb.add_kb("kb1", "First knowledge base")
    kb.select_kb("kb1")
    kb.add_composite_node("composite1_link", "composite1_name",  {"data":"composite1_data"},"composite1_description")
   
    kb.add_leaf_node( "info1_link", "info1_name",  {"data":"info1_data"},"info1_description")

    kb.leave_composite_node("composite1_link", "composite1_name")
 
    kb.add_composite_node("composite2_link", "composite2_name",  {"data":"composite2_data"},"composite2_description")
    kb.add_leaf_node("info2_link", "info2_name",  {"data":"info2_data"},"info2_description")
    
    kb.leave_composite_node("composite2_link", "composite2_name")
  
    kb.add_kb("kb2", "Second knowledge base")
    kb.select_kb("kb2")
    kb.add_composite_node("composite1_link", "composite1_name",  {"data":"composite1_data"},"composite1_description")
   
    
    kb.add_leaf_node( "info1_link", "info1_name",  {"data":"info1_data"},"info1_description")
  

    kb.leave_composite_node("composite1_link", "composite1_name")
   
    kb.add_composite_node("composite2_link", "composite2_name",  {"data":"composite2_data"},"composite2_description")
    kb.add_leaf_node("info2_link", "info2_name",  {"data":"info2_data"},"info2_description")
    
    kb.leave_composite_node("composite2_link", "composite2_name")
   
    
    # Example of check_installation
    try:
        kb.check_installation()
        kb.export_to_yaml("kb.yaml")
        
    except RuntimeError as e:
        print(f"Error during installation check: {e}")
    print("ending unit test")


import copy

class Generate_Bit_Field_Nodes:
    def __init__(self, conn,construct_kb, BitfieldDefinitionManager):
        self.conn = conn
        self.constr_kb = construct_kb
        self.BitfieldDefinitionManager = BitfieldDefinitionManager
        self.bit_flags = {}
        
    def generate_bit_field_nodes(self,name,description):
        temp_path = self.constr_kb.path[self.constr_kb.working_kb]
        label = ""
        temp_string = ".".join(temp_path)
        label = "KB_BIT_FIELD_NODE"
        node_name = temp_string +"."+label+"."+name
        node_name = node_name.replace(".", "_").lower()
        bit_table_class = self.BitfieldDefinitionManager(self.conn, node_name, description)
        
        bit_table_class.define_flags(self.bit_flags)
        bit_table_class.create_table(
            additional_columns={
                'name': 'TEXT',
                'description': 'TEXT',
                'data': 'JSONB'
            },
            primary_key='path LTREE',
            flags_column='status_flags'
        )
        
        properties = {"bit_table":node_name}
        data = {}
        self.constr_kb.add_info_node(label, name, properties, data,description)
        
    
    def clear_bit_flags(self):
    
        self.bit_flags = {}
    
    def set_bit_flags(self,flag_name, bit_position, description=""):
        if not isinstance(bit_position, int):
            raise ValueError("Bit position must be an integer")
        if not isinstance(description, str):
            raise ValueError("Description must be a string")
        if not isinstance(flag_name, str):
            raise ValueError("Flag name must be a string")
        
        self.bit_flags[flag_name] = {'bit': bit_position, 'description': description}
 

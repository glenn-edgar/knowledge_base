# Outline

1.  The Need for Graph Based Knowledge Systems
2.  Use Tree Based Knowledge System
3.  Python functions for construction a tree based knowledge system
4.  Python functions for searching for knowledge based entities

## Appendixes
A.  Description of Johnsons Control BAS system -- *separate document*

---

# 1. The Need for Graph Based Knowledge Systems

Graph-based knowledge systems are increasingly fundamental to edge computing across diverse domains. Below is a refined overview of how these systems are deployed in real-world edge environments, followed by a detailed, illustrative example from a smart factory context.

## Applications of Graph-Based Knowledge Systems at the Edge

* **Smart Home and Building Automation Systems**
    * Johnson Controls has implemented knowledge graph technology for their building automation systems (BAS), creating an enhanced integration between physical devices and digital data¹. This approach:
        * Ingests heterogeneous data from disparate sources (sensors, occupancy data, manufacturer's data)
        * Creates a digital twin of each controller, space, and event in a building
        * Enables better integration between systems and richer dashboards
        * Optimizes energy consumption through improved data relationships
    * Description of BAS in Appendix A.

* **Industrial IoT (IIoT) Platforms**
    * Leading industrial companies such as Siemens and Bosch integrate knowledge graphs into their IIoT platforms to model complex relationships among machines, sensors, and tools²⁵⁹. This enables advanced applications like predictive maintenance, asset tracking, and process optimization by providing a unified, semantically rich view of industrial assets and their interconnections⁵⁹¹⁷.

* **Data Center Infrastructure Management (DCIM)**
    * DCIM providers, including Schneider Electric and Nlyte, utilize graph-based models to represent the physical and logical layout of data centers—covering equipment, cabling, and dependencies⁶¹⁴¹⁰. This approach supports visualization, dependency analysis, power and cooling management, and capacity planning, all of which are critical for efficient data center operations⁶¹⁴¹⁰¹⁸.

* **Smart City Platforms**
    * Cities such as Amsterdam, Singapore, and Vienna have adopted knowledge graphs to integrate data from urban infrastructure, including traffic sensors, public transportation, and energy systems⁷¹⁵¹⁹. These graph-based systems facilitate real-time traffic optimization, energy management, and coordinated emergency response by connecting disparate data sources for holistic analysis⁷¹⁵¹⁹¹¹.

* **Building Management Systems (BMS)**
    * Organizations like Johnson Controls and Honeywell employ graph databases to model building structures and their subsystems—HVAC, lighting, security, and more⁷. This enables smarter automation, energy optimization, and improved safety by providing a comprehensive, interconnected view of building operations⁷.

## Detailed Graph Example: Smart Factory

### Scenario
A smart factory leverages interconnected machines, sensors, and robots to optimize production, enable predictive maintenance, and enhance safety.

### Graph Representation
The factory’s components and their relationships are modeled as a graph, where:

* **Nodes** represent entities such as machines, sensors, robots, and areas, each with relevant properties (e.g., machine type, sensor frequency, robot task).
* **Edges** define relationships, such as which sensor is attached to which machine, where equipment is located, and which robots serve specific areas.

### Node Types and Example Properties:
* **Machine**: `machine_id`, `type` (e.g., CNC, AssemblyRobot), `location`, `status` (e.g., Running, Idle, Fault), `ip_address`
* **Sensor**: `sensor_id`, `type` (e.g., Temperature, Vibration), `measurement_unit`, `location`, `data_frequency`
* **Area**: `area_id`, `name` (e.g., Assembly Line 1), `coordinates`
* **Robot**: `robot_id`, `type` (e.g., AGV), `current_task`, `speed`

### Edge Types:
* `Attached_to`: Connects a sensor to a machine
* `Located_in`: Connects machines, sensors, and robots to areas
* `Monitors`: Connects a sensor to the machine it monitors
* `Communicates_with`: Connects a machine to an edge device
* `Serves`: Connects a robot to the area it operates in

### Conceptual Example:
```text
[Area A (A1, Assembly Line 1)]
   |--Located_in--> [Machine 1 (M1, CNC, Running)]
   |     |--Attached_to--> [Sensor 101 (S101, Temperature)]
   |     |--Monitors--> [Sensor 101]
   |--Located_in--> [Sensor 101]
   |--Located_in--> [Robot 1 (R1, AGV)]
     |--Serves--> [Robot 1]

[Area B (A2, Welding Station)]
   |--Located_in--> [Machine 2 (M2, AssemblyRobot, Idle)]
   |     |--Attached_to--> [Sensor 201 (S201, Vibration)]
   |--Located_in--> [Sensor 201]
Benefits in Edge Systems:
Rapid Information Retrieval: The graph structure allows quick queries about equipment status, location, and dependencies, supporting real-time decision-making¹³.
Contextual Data Processing: Sensor data can be analyzed in the context of the machine and its location, improving the accuracy of diagnostics and predictions³⁴.
Relationship Awareness: Understanding how components interact enables more effective automation, maintenance scheduling, and safety monitoring²⁵¹³.
Summary
Graph-based knowledge systems empower edge computing by modeling complex, real-world relationships in industrial, infrastructure, urban, and building environments. Their ability to unify diverse data sources, represent dependencies, and support intelligent decision-making makes them indispensable for modern edge applications—from smart factories to smart cities.

2. Tree Based Knowledge System
Graph-based systems, while powerful, can be quite resource intensive. For instance, a GraphDB instance typically requires a substantial memory footprint, ranging from approximately 500 MB to 1 GB. In contrast, a general-purpose database like PostgreSQL, with a comparable container size of around 500 MB, can implement a tree-based knowledge base by leveraging its ltree extension. Furthermore, PostgreSQL offers the distinct advantage of being a versatile database solution suitable for a wide array of other services beyond just knowledge representation.

However, a tree-based approach inherently imposes limitations compared to the flexibility of a graph model. The primary restriction lies in its hierarchical structure, where each node (except the root) has a single parent. This makes it challenging to represent complex relationships involving multiple connections between entities, such as many-to-many relationships or cyclical dependencies, which are naturally handled in graph databases.

Despite these limitations, workarounds exist for representing more intricate relationships within a tree structure. One common approach involves introducing intermediary nodes to represent relationships. For example, a many-to-many relationship between "Authors" and "Books" could be modeled with an intermediary "Writes" node connecting them. Each author would have a "Writes" child node for each book they've written, and each book would have a "Writes" child node for each author. Another workaround involves using encoded strings or structured data within a single node's attribute to represent multiple relationships. However, these workarounds often add complexity to querying and maintaining the knowledge base compared to the direct and intuitive representation offered by graph models.

The system's knowledge base is stored in a table named knowledge_base, designed to represent nodes within a hierarchical tree structure. The table schema is as follows:

SQL

CREATE TABLE knowledge_base (
    id SERIAL PRIMARY KEY,
    label VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    properties JSON,
    data JSON,
    path LTREE
);
Each entry in the knowledge_base table represents a single node in the knowledge tree. The columns are defined as follows:

id: A unique serial identifier for each node.
label: A label that connects the node to its position within the knowledge base tree.
name: The name of the node.
properties: A JSON field used to store key-value pairs representing node properties. This field is specifically designed for efficient graph searches.
data: A JSON field used to store general node data. Note the distinction: properties is optimized for search, while data stores arbitrary information.
path: An LTREE column that represents the hierarchical path of the node within the tree. The LTREE values are structured as top_label.top_name.....current_label.current_name, effectively encoding the node's lineage.
The search capabilities of the ltree field can be summarized as follows:

The ltree extension in PostgreSQL provides several operators and functions to efficiently search and manipulate hierarchical data. Here are the main search properties:

Operators:

@> (Ancestor): Checks if one ltree value is an ancestor of another.
left @> right: Returns true if left is an ancestor of right (or equal).
<@ (Descendant): Checks if one ltree value is a descendant of another.
left <@ right: Returns true if left is a descendant of right (or equal).
~ (Match): Checks if an ltree value matches an lquery pattern.
ltree ~ lquery or lquery ~ ltree: Returns true if the ltree value matches the lquery pattern. lquery allows for wildcard matching and other pattern-matching features.
? (Match any): Checks if an ltree value matches any of the lquery patterns in an array.
ltree ? lquery[] or lquery[] ? ltree: Returns true if the ltree value matches any of the lquery patterns in the array.
@ (Match ltxtquery): Checks if an ltree value matches an ltxtquery pattern.
ltree @ ltxtquery or ltxtquery @ ltree: Returns true if the ltree value matches the ltxtquery pattern. ltxtquery is used for full-text search-like matching within the ltree values.
Functions:

subltree(ltree, int start, int end): Extracts a subpath from an ltree value.
subpath(ltree, int offset, int len) / subpath(ltree, int offset): Extracts a subpath from an ltree value, with options for specifying the starting position and length.
nlevel(ltree): Returns the number of labels in an ltree path.
index(ltree a, ltree b) / index(ltree a, ltree b, int offset): Returns the position of the first occurrence of a subpath within an ltree value.
lca(ltree, ltree, ...) / lca(ltree[]): Finds the longest common ancestor of the given ltree paths.
Key Search Properties:

Hierarchical Relationships: The core strength of ltree is the ability to efficiently query hierarchical relationships using operators like @> and <@. You can easily find ancestors, descendants, and subtrees.
Pattern Matching: The lquery data type allows for flexible pattern matching within the ltree paths using wildcards and quantifiers.
Full-Text Search: The ltxtquery data type enables full-text search-like capabilities, allowing you to find ltree values that contain specific words or phrases, regardless of their position in the path.
Subpath Extraction: Functions like subltree and subpath allow you to extract portions of the ltree paths, which can be useful for navigating and manipulating the hierarchy.
Path Length: The nlevel function allows you to determine the depth of a node in the hierarchy.
Common Ancestor: The lca function is useful for finding the common root of different branches in the tree.
These features make ltree a powerful tool for working with hierarchical data in PostgreSQL, enabling efficient and flexible search operations.

3. Python Functions for Constructing a Tree-Based Knowledge System
The tree-based knowledge base is constructed using the Python class Construct_KB.

Python

class Construct_KB:
    """
    This class is designed to construct a knowledge base structure with header
    and info nodes, using a stack-based approach to manage the path.  It also
    manages a connection to a PostgreSQL database and sets up the schema.
    """
This class provides the following key functionalities:

__init__(self, host, port, dbname, user, password, database)
Python

    def __init__(self, host, port, dbname, user, password, database):
        """
        Initializes the Construct_KB object and connects to the PostgreSQL database.

        Args:
            host (str): The database host.
            port (str): The database port.
            dbname (str): The name of the database.
            user (str): The database user.
            password (str): The password for the database user.
            database (str):  (Redundant with dbname, but kept for compatibility)

        Deletes the current knowledge base table if present.
        Creates a new knowledge base table with the following schema:
            CREATE TABLE knowledge_base (
                id SERIAL PRIMARY KEY,
                label VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                properties JSON,
                data JSON,
                path LTREE
            );
        Also sets up the database schema.
        The table rows are explained in a previous section.
        """
The constructor establishes a connection to the PostgreSQL database using the provided credentials. It then deletes any existing knowledge_base table and creates a new one with a predefined schema. This schema includes fields for node ID, label, name, properties (for graph searches), data (for general storage), and an LTREE path to represent the hierarchical structure.

add_header_node(self, link, node_name, node_properties, node_data)
Python

    def add_header_node(self, link, node_name, node_properties, node_data):
        """
        Adds a header node to the knowledge base.

        Args:
            link: The link associated with the header node.
            node_name: The name of the header node.
            node_properties: Properties associated with the header node.
            node_data: Data associated with the header node
        """
This method adds a header node to the knowledge base. Header nodes likely represent parent nodes in the hierarchy.

add_info_node(self, link, node_name, node_properties, node_data)
Python

    def add_info_node(self, link, node_name, node_properties, node_data):
        """
        Adds an info node to the knowledge base.  An info node is a child node and has no dependent
        This function adds a node and then immediately removes its link and name from the path.
        It now checks that the path has a length greater than 1 before adding.

        Args:
            link: The link associated with the info node.
            node_name: The name of the info node.
            node_properties: Properties associated with the info node.
            node_data: Data associated with the header node.
        """
This method adds an info node (child node) to the knowledge base. It differs from add_header_node in that it adds a node and then removes its link and name from the path. It also ensures the path length is greater than 1 before adding the node.

leave_header_node(self, label, name)
Python

    def leave_header_node(self, label, name):
        """
        This function leaves the scope of the parent node.  The argument label and name must
        match the label and name of the parent node of whose scope is being left.
        This function ensures that trees are topologically correctly constructed.

        Args:
            label: The expected link of the header node.
            name: The expected name of the header node.
        """
This method is used to exit the scope of a parent node, ensuring the correct topological construction of the tree. The provided label and name must match the parent node being left.

check_installation(self)
Python

    def check_installation(self):
        """
        Checks if the installation is correct by verifying that the path is empty
        and disconnecting from the PostgreSQL database.
        If the path is not empty, delete the knowledge_base table and then raises an error.
        """
This method verifies the integrity of the knowledge base construction. It checks if the path is empty and disconnects from the database. If the path is not empty, it indicates an incomplete or incorrect construction, and the method deletes the knowledge_base table and raises an error.

4. Python Functions for Searching Knowledge-Based Entities
KB_Search Class Documentation
Overview
The KB_Search class offers a robust and flexible interface for querying the knowledge_base table within a PostgreSQL database. Leveraging advanced PostgreSQL features like Common Table Expressions (CTEs), JSONB property querying, and LTREE path expressions, it empowers developers to build sophisticated knowledge base and metadata-driven applications with composable filters.

Class: KB_Search
A Python class designed to facilitate structured querying of the knowledge_base table in PostgreSQL. It enables dynamic construction of complex SQL queries through composable filters, supporting JSONB field searches and LTREE path matching. The class employs CTEs to create modular and reusable query segments and always retrieves all columns from the target table.

Initialization
Python

def __init__(self)
Initializes a new KB_Search object. This sets up the internal state for managing filters, the database connection, and query results.

Attributes:

base_table (str): The name of the PostgreSQL table to query. Defaults to "knowledge_base".
filters (list[dict]): A list storing the filter conditions to be applied to the query. Each filter is a dictionary containing the SQL WHERE clause fragment and its corresponding parameters.
conn (psycopg2.extensions.connection or None): The active PostgreSQL database connection object. Initialized as None.
cursor (psycopg2.extensions.cursor or None): The database cursor object used for executing SQL queries. Initialized as None.
results (list[dict] or None): Stores the results (list of dictionaries) of the most recently executed query. Initialized as None.
Database Connection
Python

def connect(self, dbname: str, user: str, password: str, host: str = "localhost", port: str = "5432") -> bool
Establishes a connection to a PostgreSQL database using the provided credentials.

Args:

dbname (str): The name of the database to connect to.
user (str): The username for database authentication.
password (str): The password for database authentication.
host (str, optional): The hostname¹ or IP address of the PostgreSQL server. Defaults to "localhost". github.com
port (str, optional): The port number the PostgreSQL server is listening on. Defaults to "5432".
Returns:
bool: True if the database connection was established successfully, False otherwise.

Python

def disconnect(self) -> bool
Closes the current database connection and releases associated resources.

Returns:
bool: True if the disconnection was successful, False otherwise.

Filter Management
Python

def clear_filters(self)
Removes all currently applied filters, effectively resetting the query to select all rows.

Python

def search_label(self, label: str)
Adds a filter to the query to select rows where the value of the label column exactly matches the provided label.

Args:

label (str): The exact label value to search for.
Python

def search_name(self, name: str)
Adds a filter to the query to select rows where the value of the name column exactly matches the provided name.

Args:

name (str): The exact name value to search for.
Python

def search_property_key(self, key: str)
Adds a filter to the query to select rows where the properties JSONB field contains the specified key at the top level.

Args:

key (str): The JSON key to check for existence.
Python

def search_property_value(self, key: str, value: Any)
Adds a filter to the query to select rows where the properties JSONB field contains the specified key-value pair. The comparison is an exact match for the given value.

Args:

key (str): The JSON key to search within.
value (Any): The value to match for the specified key. This value will be automatically serialized to JSON if necessary.
Python

def search_path(self, path_expression: str)
Adds a filter to the query to select rows where the path column (assumed to be of LTREE type) matches the given path_expression. This leverages PostgreSQL's LTREE operators for hierarchical data querying.

Args:

path_expression (str): The LTREE path expression to match.
Examples:

'docs.technical' - Matches the exact path docs.technical.
'docs.*' - Matches all immediate children of the docs node.
'docs.*{1,3}' - Matches descendants of docs up to 3 levels deep.
'docs.technical.*' - Matches all descendants of the docs.technical node.
'*.technical.*' - Matches any path that contains technical.
Query Execution
Python

def execute_query(self) -> bool
Constructs and executes the SQL query based on the filters added using the search_* methods. The filtering is performed progressively using a chain of Common Table Expressions (CTEs) for enhanced composability and reentrancy.

Returns:
bool: True if the query executed successfully, False otherwise.

Behavior:

If no filters have been added, a simple SELECT * FROM knowledge_base query is executed, retrieving all rows.
When filters are present, a series of CTEs is generated. Each CTE represents the application of a single filter to the results of the previous CTE.
Parameter naming is handled automatically to prevent conflicts between different filter parameters.
The results of the executed query are stored in the self.results attribute.
Results Retrieval
Python

def get_results(self) -> list[dict]
Returns the results of the most recently executed query.

Returns:
list[dict]: A list of dictionaries, where each dictionary represents a row from the query result. Returns an empty list if no query has been executed or if the last query returned no results.

Example Usage
Python

kb = KB_Search()
if kb.connect(dbname="mydb", user="user", password="pass"):
    kb.search_label("FAQ")
    kb.search_property_key("author")
    kb.search_path("docs.*")
    if kb.execute_query():
        results = kb.get_results()
        for row in results:
            print(row)
    kb.disconnect()
Notes
This class requires the psycopg2 and psycopg2.extras libraries to be installed for PostgreSQL database interaction.
It is assumed that the properties column in the knowledge_base table is of the JSONB data type, and the path column is of the LTREE data type.
All filters added using the search_* methods are composable and can be cleared using the clear_filters() method.
Error messages related to database connection and query execution are printed to the standard output.
This documentation provides a comprehensive guide for utilizing the KB_Search class to perform advanced knowledge base queries in PostgreSQL.
Explanation of Filter System
The KB_Search class employs a sophisticated filtering mechanism based on PostgreSQL Common Table Expressions (CTEs) to combine multiple search criteria effectively. Filters are applied sequentially, with each filter operating on the output of the preceding one, creating a progressive refinement of the result set.

Filter Mechanism Architecture
The core of the filtering system lies in the dynamic generation of a chain of CTEs. Each added filter translates into a new CTE that filters the data produced by the previous CTE.

Key Components:
Filter Storage: Filters are stored as dictionaries within the self.filters list. Each dictionary encapsulates:
condition (str): A fragment of the SQL WHERE clause representing the filter logic.
params (dict): A dictionary of parameters to be used with the condition for safe SQL execution using psycopg2's parameter substitution.
Filter Types: The KB_Search class provides methods for various filter types:
Direct Value Matches (search_label, search_name): These methods create simple WHERE clauses for exact string matching.
SQL

-- Example: search_label("FAQ")
WHERE label = %(p0_label)s
JSONB Property Checks (search_property_key, search_property_value): These methods utilize PostgreSQL's JSONB operators to query within the properties column.
SQL

-- search_property_key("author")
WHERE properties::jsonb ? %(p1_property_key)s

-- search_property_value("status", "published")
WHERE properties::jsonb @> %(p2_properties)s
LTREE Path Queries (search_path): This method leverages PostgreSQL's LTREE operators for pattern matching on hierarchical data stored in the path column.
SQL

-- search_path("docs.technical.*")
WHERE path ~ %(p3_path)s
Query Construction Process:
Base Case: If no filters are added, the query is simply:
SQL

SELECT * FROM knowledge_base;
Progressive Filtering: For each added filter, a new CTE is generated. The first filter operates on the knowledge_base table, and subsequent filters operate on the results of the previous filter's CTE.
SQL

WITH
base_data AS (SELECT * FROM knowledge_base),
filter_0 AS (SELECT * FROM base_data WHERE label = %(p0_label)s),
filter_1 AS (SELECT * FROM filter_0 WHERE properties::jsonb ? %(p1_property_key)s),
filter_2 AS (SELECT * FROM filter_1 WHERE path ~ %(p2_path)s)
SELECT * FROM filter_2;
Parameter Handling: To prevent naming collisions when multiple filters are applied, unique parameter names are generated using a prefix (e.g., p0_label, p1_property_key). The params dictionary associated with each filter is merged into a single dictionary with these prefixed keys before executing the query.
Execution Flow:
Filter Composition: When search_* methods are called, they append a dictionary containing the SQL condition and its parameters to the self.filters list.
Python

kb.search_label("FAQ")  # Adds {"condition": "label = %(label)s", "params": {"label": "FAQ"}}
kb.search_property_key("author")  # Adds {"condition": "properties::jsonb ? %(property_key)s", "params": {"property_key": "author"}}
CTE Chain Generation: The execute_query() method iterates through the self.filters list and constructs the SQL query with the chain of CTEs as described above.
Parameter Transformation: The individual params dictionaries from each filter are combined into a single dictionary with unique prefixed keys for safe parameter substitution during query execution.
Python

# Original filters' params:
[{"label": "FAQ"}, {"property_key": "author"}]
# Transformed params for execute():
{"p0_label": "FAQ", "p1_property_key": "author"}
Key Design Features:
Composable Filters: Filters can be added in any order, and each filter progressively refines the result set based on the output of the previous filter.
Safe Parameter Handling: By utilizing psycopg2's parameter substitution mechanism, the class effectively prevents SQL injection vulnerabilities.
Automatic JSON Serialization: When using search_property_value, Python objects provided as the value are automatically serialized to JSON strings for accurate comparison within the JSONB field.
LTREE Optimization: The search_path method leverages PostgreSQL's built-in LTREE extension, enabling efficient querying of hierarchical data structures using specialized path matching operators.
JSONB Flexibility: The search_property_key and search_property_value methods provide powerful tools for querying semi-structured data stored in the properties JSONB column, supporting both key existence checks and value-based filtering.
This design empowers developers to construct complex and targeted queries through a simple and intuitive API, while ensuring database security and leveraging the performance optimizations offered by PostgreSQL's advanced data types and features.

This refined version aims to be more explicit, consistent in formatting, and provides a deeper dive into the filter system's architecture and behavior. The code examples within the documentation are syntactically correct and clearly illustrate the usage of each method.

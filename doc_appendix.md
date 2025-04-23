# Appendix A. Description of Johnsons Control BAS System

## Johnson Controls Building Automation System (BAS)

Johnson Controls offers a comprehensive suite of building automation systems designed to integrate and control various building functions through a centralized platform. Their solutions combine advanced technology with practical functionality to create efficient, sustainable, and comfortable building environments.

### Core Systems and Platforms

Johnson Controls' flagship BAS offerings include:

* **`Metasys`**: A world-class integrated building management system that seamlessly connects HVAC, lighting, fire, security, and other building systems through a single, easy-to-navigate platform³⁴. As one of the Top-5 Integrated Building Management Systems, `Metasys` has been enhanced with semantic data capabilities through `GraphDB` technology to provide advanced data management³.
* **`Facility Explorer`**: A more powerful front-end system compared to legacy `Metasys` interfaces, providing enhanced analytics and control capabilities⁵. This system seamlessly integrates with automation systems to optimize performance and energy efficiency across facilities⁵.
* **Light Commercial Building Controls**: The company offers plug-and-play systems specifically designed for light commercial buildings that allow configuration of multiple applications without requiring extra tools⁴⁶.

### Technical Architecture

The Johnson Controls BAS architecture incorporates several key components:

* **Knowledge Graph Technology**: The system utilizes a semantic layer that creates a digital twin of each controller, space, and event in a building³. This approach enables better integration between physical devices and digital data³.
* **Communication Protocols**: Older Johnson systems use the proprietary `N2` protocol for communication between equipment controllers and the network's front end⁵. The system is considered both open and proprietary—it allows integration of any manufacturer's equipment but requires Johnson controllers to communicate with the network⁵.
* **Controllers**: Johnson Controls offers various controllers, including the `JCI Metasys N2` controllers. For legacy systems, upgrades can be implemented using a `Tridium JACE` controller fitted with a Johnson-developed communications driver⁵.

### Key Features and Capabilities

Johnson Controls BAS provides a wide range of functionalities:

* **Integration Capabilities**:
    * Extends automated control to every building system—HVAC, lighting, security, and detection—on a single platform²⁶
    * Enables seamless interoperability between diverse building systems²
    * Allows for configurable controls that integrate with smart equipment²⁶
* **Data Management**:
    * Ingests heterogeneous data from disparate sources (sensors, occupancy data, manufacturer's data)³
    * Provides seamless data integration and easy management through knowledge graph technology³
    * Helps pinpoint problems and tame complex data streams⁴
* **User Interface**:
    * Offers a graphical, web-based front end that makes system management intuitive⁵
    * Provides richer dashboards and enhanced user interfaces through improved data linking³
    * Allows facility managers to easily optimize environments for occupant comfort and safety⁵
* **Energy and Operational Efficiency**:
    * Optimizes energy consumption through improved control and monitoring capabilities³⁵
    * Enables cost reduction through more efficient building management³
    * Allows for tailored programming that enhances energy savings potential⁵
* **Additional Functionality**:
    * Environment controls include accurate airflow measurement and room pressure controllers²⁶
    * Comprehensive solutions for food safety and refrigeration control⁶
    * Security integration to protect people, facilities, and assets²⁶

### Evolution and Upgradeability

Johnson Controls BAS systems have evolved over time:

* Legacy systems like `JCI DSC-8500` are now considered outdated and have controllers that are no longer supported⁵
* More recent systems using `JCI Metasys N2` controllers can be upgraded with newer front-end interfaces like `Facility Explorer`⁵
* Upgrades provide more functionality with customizable options for equipment control and analytics⁵
* Modern versions incorporate semantic technology to enhance building management capabilities³

Johnson Controls continues to innovate in the building automation space, with their systems increasingly focusing on creating safer, smarter buildings that are easier to manage while optimizing for energy efficiency and occupant comfort.

---

## Structure of Building Automation System (BAS) Knowledge Graphs

Building Automation System (BAS) knowledge graphs provide a comprehensive semantic framework that connects disparate building systems into an integrated, machine-interpretable model. These graphs serve as the foundation for modern smart building management by creating relationships between physical and digital elements.

### Core Components and Relationships

A typical BAS knowledge graph consists of several key elements:

* **Facilities nodes**: Represent entire buildings or distinct sections within a complex⁴
* **Equipment nodes**: Model HVAC units, lighting systems, security systems, and other physical devices⁴
* **`BACnet` objects**: Represent parameters and data points collected from various sensors and controllers⁴
* **Controllers**: Digital control units that manage specific building functions⁵
* **Spaces**: Physical areas within the building (rooms, zones, floors)⁵
* **Events**: Temporal activities or incidents occurring within the building systems⁵

These components are connected through semantic relationships that reflect their real-world interactions. In more advanced implementations, a single BAS knowledge graph might contain thousands of entities - for example, Smart-Buildings.io's implementation includes approximately 2,800 facilities, 3,700 pieces of equipment, and 4,000 `BACnet` objects⁴.

### Ontological Framework

BAS knowledge graphs typically use established ontologies to structure their data:

* **`BrickSchema`**: A popular ontology specifically designed for modeling smart building components³
* **`QUDT` ontology**: Used for handling quantities, units, dimensions, and types²
* **Custom extensions**: Organizations often extend standard ontologies to meet specific requirements²

The semantic model enables a standardized way to describe building systems across different manufacturers and protocols. For example, Johnson Controls' system creates a digital twin of each controller, space, and event in a building to enable better integration between physical devices and digital data⁵.

### Data Integration Layer

A key function of BAS knowledge graphs is integrating heterogeneous data from multiple sources:

* Real-time sensor data from building systems
* Occupancy information from access control systems
* Manufacturer specifications for installed equipment
* Building Information Models (`BIM`) from the design phase
* Requirement definitions and engineering specifications
* Digital product catalogs and parts lists²

This integration allows for a holistic view of the building, breaking down information silos that traditionally exist between different building systems and phases of the building lifecycle².

### Semantic Tagging System

BAS knowledge graphs implement semantic tagging to enable complex automation:

* Contextual modeling of building entities
* Rich semantic definitions of building automation components
* Tags that describe functionality, location, and relationships
* Metadata that standardizes descriptions across heterogeneous systems²

This tagging system is a fundamental enabler for automating engineering tasks in the building domain, allowing systems to understand relationships between components without human intervention².

### Implementation Examples

Several major building automation companies have implemented knowledge graph technology:

* **Johnson Controls**: Uses `GraphDB` to power their `Metasys` system, creating a semantic layer that enhances building management capabilities⁵
* **Schneider Electric**: Implements `GraphDB` with `BrickSchema` for their `StruxureWare Building Operation` system³
* **Smart-Buildings.io**: Uses `Memgraph` to create facility indexes and digital twins, connecting thousands of facilities and devices⁴

In practical applications, these knowledge graphs significantly improve system performance - for example, Smart-Buildings.io reported reducing API calls by two-thirds and substantially improving processing time by implementing their graph database solution⁴.

BAS knowledge graphs represent an evolution in building automation, transforming disconnected systems into integrated networks that can truly make buildings "think for themselves" through improved data management, semantic understanding, and relationship modeling.

---

## What is GraphDB

`GraphDB` refers to both a specific product by `Ontotext` and a broader category of database management systems that use graph structures to store and query data. Unlike traditional relational databases that use tables, graph databases emphasize relationships between data entities through a network structure of nodes and edges.

### Fundamental Structure of Graph Databases

Graph databases are built on graph theory concepts and consist of three primary elements:

* **Nodes (Vertices)**: These are the data entities or objects in the database. Nodes can be tagged with labels representing their roles and can hold any number of key-value pairs as properties⁷. For example, in a social network graph, people would be represented as nodes.
* **Edges (Relationships)**: These connect nodes and represent relationships between them. Edges always have a start node, an end node, exactly one type, and a direction⁷. They can also have properties that describe the nature of the relationship¹⁵.
* **Properties**: These are attributes that describe both nodes and edges, providing additional information about the entities and their relationships¹². In property graphs, both nodes and edges can have these descriptive attributes¹.

### Types of Graph Databases

There are two popular models of graph databases:

* **Property Graphs**: Focus on analytics and querying capabilities. They store data as a collection of nodes and the relationships between them, with properties attached to both⁶.
* **`RDF` Graphs (Resource Description Framework)**: Emphasize data integration across different sources. They store data as triples (subject-predicate-object) and are particularly useful for semantic web applications⁶.

### Ontotext GraphDB Specifically

`Ontotext GraphDB` is a specific implementation of a graph database that:

* Functions as a fully semantic graph database with `RDF` and `SPARQL` support³
* Complies with W3C standards for data representation and querying³⁴
* Specializes in building knowledge graphs by linking diverse business data³
* Offers semantic inferencing at scale, deriving new semantic facts from existing ones⁴
* Provides high availability through replication clusters for enterprise use cases⁴

### Technical Advantages

Graph databases offer several technical advantages:

* **Index-free adjacency**: Many graph databases use this approach where nodes directly reference adjacent nodes, allowing for fast traversal without needing to consult an index⁵.
* **Relationship priority**: By storing relationships permanently in the database structure, querying relationships becomes significantly faster compared to relational databases⁵.
* **Flexibility**: Graph schemas can evolve without disrupting existing functionality, making them adaptable to changing business needs⁵⁷.
* **Scalability**: Modern graph databases can scale to billions of nodes while maintaining performance⁷.

### Applications and Use Cases

Graph databases are particularly valuable for:

* **Knowledge Graphs**: Building comprehensive representations of information and their relationships³⁴
* **Complex Networks**: Modeling social networks, organizational structures, and other relationship-rich data²⁵
* **Recommendation Systems**: Leveraging connections between users and products to generate recommendations²
* **Semantic Search**: Enabling context-aware search capabilities across connected information³⁴
* **Data Integration**: Unifying data from multiple sources into a coherent structure³⁴
* **AI and `LLM` Enhancement**: Providing structured knowledge to power artificial intelligence applications³

Graph databases represent a powerful paradigm for dealing with highly connected data where relationships are as important as the data itself. They excel at queries that would require multiple joins in relational databases, offering an intuitive and efficient way to work with complex, interconnected information systems⁵.

---

## GraphDB Free: Features and Capabilities

`GraphDB Free` is a fully functional semantic graph database offered by `Ontotext` as a free version of their enterprise-ready `GraphDB` product. It serves as an excellent entry point for those looking to explore semantic graph database technology without financial commitment.

### What is GraphDB Free?

`GraphDB Free` is a semantic graph database (also called an `RDF` triplestore) that's compliant with W3C standards. It provides core infrastructure for solutions where:

* Modeling agility is important
* Data integration across sources is needed
* Relationship exploration is a key requirement
* Cross-enterprise data publishing and consumption are essential¹

> As described by Software Informer, "GraphDB Free is a perfect starting point for smart data proof-of-concepts and for projects that require an on-premise or embedded semantic graph database."⁵

### Features Included in GraphDB Free

`GraphDB Free` comes with several powerful capabilities:

* Full `RDF` and `SPARQL` support for semantic queries
* Compliance with W3C standards for data representation
* On-premise or embedded deployment options (cloud options are also available)
* `Lucene` connector for enhanced search capabilities
* Core functionality of the `GraphDB` platform¹⁵

### Limitations of GraphDB Free

The free version does have some restrictions compared to paid editions:

* Limited to a single query in parallel
* Lacks some of the connectors available in other editions
* May have limitations on scalability and resilience compared to Enterprise version¹

### Comparison to Other GraphDB Editions

`GraphDB` is available in multiple editions:

* **`GraphDB Free`**
    * Free to use
    * `Lucene` connector
    * Limited to a single query in parallel
* **`GraphDB Enterprise`**
    * Includes all `GraphDB Free` features
    * Supports unlimited parallel queries
    * Offers more scalability and resilience
    * Additional connectors: `Elasticsearch`, `OpenSearch`, `Solr`, `Kafka`¹

### Other Free Graph Database Options

For context, there are several other free graph database options available:

* **`Neo4j AuraDB Free`**
    * Fully managed cloud database
    * Free forever with no credit card required
    * Limited to 50,000 nodes and 175,000 relationships
    * Automatically pauses after three days of inactivity
    * Includes `Neo4j Bloom` for data visualization³
* **Open Source Alternatives**
    * Various open-source graph databases are available, including options like `ArangoDB`, `Dgraph`, `OrientDB`, and `Cayley`
    * Many offer community editions or trial periods (for example, one option mentioned provides a 60-day free trial)⁴

### How to Get GraphDB Free

`GraphDB Free` is available for download from the `Ontotext` website. The company offers various distributions, including:

* Direct download
* `Maven` repository access
* `Docker` containers
* `Helm` charts for `Kubernetes` deployment¹

`GraphDB Free` represents an excellent opportunity to explore semantic graph database technology for proof-of-concept projects, learning environments, or smaller-scale applications that don't require the advanced parallel processing capabilities of the Enterprise edition.

---

## GraphDB Container Size and Parallel Queries

### GraphDB Container Size

The size of `GraphDB` Docker containers varies depending on the specific version and configuration. Based on the provided Docker setup examples:

* The official `Ontotext GraphDB` image for version `10.7.0` has a base size of approximately **500MB-1GB** when pulled from Docker Hub.
* Container size increases with additional data volumes and configurations:
    * Default data directory: `/opt/graphdb/home`
    * Default import directory: `/root/graphdb-import`
    * License files and custom configurations add minimal overhead
* Key size considerations:
    * Base image uses `OpenJDK 11` as foundation
    * Includes `GraphDB` runtime environment and dependencies
    * Data storage scales independently through volume mounts
    * Prebuilt images optimize layer caching for efficient updates

To check the exact size for a specific version:

```bash
docker pull ontotext/graphdb:10.7.0
docker images | grep graphdb
Parallel Queries in GraphDB
A parallel query is an execution strategy that processes multiple query components simultaneously using concurrent resources. In GraphDB and other databases, this involves:

Core Mechanism

Query decomposition: Splitting queries into independent sub-tasks
Resource allocation: Distributing tasks across multiple CPU cores/threads
Result aggregation: Combining partial results into final output
Implementation in GraphDB

Morsel-driven parallelism (as referenced in Search Result 4):
Divides work into "morsels" (small data units)
Dynamically assigns morsels to worker threads
Particularly effective for recursive joins in graph traversals
Partition-based retrieval (shown in Oracle example):
Java

// Parallel vertex retrieval across partitions
Iterable<Vertex>[] iterables = opg.getVerticesPartitioned(
    conns,         // Multiple database connections
    true,          // Skip cache
    partitionID    // Data partition index
);
Execution optimization:
Uses statistics about graph structure and data distribution
Implements cost-based query planning
Balances workload across available resources
Performance Impact

Enterprise Edition: Supports unlimited parallel queries
Free Edition: Limited to single parallel query execution
Typical speed improvements (from Search Result 4):
2-10x faster for complex graph patterns
Linear scaling with core count up to physical limits
Use Cases

Large-scale graph traversals
Batch RDF data imports using importrdf tool
Concurrent analytical queries on knowledge graphs
Real-time recommendation systems
Example parallel query pattern:

Plaintext

# Hypothetical SPARQL extension example
SELECT ?path WHERE {
    ?start :connectedTo* ?end
} USING PARALLEL 8
This SPARQL extension (hypothetical example) would distribute the pathfinding operation across 8 worker threads.

Both container sizing and parallel query capabilities make GraphDB particularly suitable for enterprise-scale knowledge graph applications requiring high throughput and efficient resource utilization.

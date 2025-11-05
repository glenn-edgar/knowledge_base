import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import numpy as np

class PostgresGeoRoutingDB:
    """
    A class to manage geospatial routing data in PostgreSQL using PostGIS and pgRouting extensions.
    Assumes the extensions are installable; creates tables for vertices (nodes) and edges (roads).
    Supports Dijkstra and A* routing, plus basic geo area queries (e.g., bounding box, buffers).
    """
    
    def __init__(self, host='localhost', port=5432, dbname='postgres', user='postgres', password='password'):
        """
        Initialize the connection to PostgreSQL.
        
        :param host: Database host.
        :param port: Database port.
        :param dbname: Database name.
        :param user: Database user.
        :param password: Database password.
        """
        self.conn_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish connection and ensure extensions."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.conn.autocommit = True
            cursor = self.conn.cursor()
            # Ensure PostGIS and pgRouting extensions
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgrouting;")
            cursor.close()
            print("Connected to PostgreSQL with PostGIS and pgRouting extensions.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """
        Create tables for vertices (points) and edges (linestrings) if they do not exist.
        Includes spatial indexes.
        """
        try:
            cursor = self.conn.cursor()
            
            # Vertices table
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS vertices (
                    id INTEGER PRIMARY KEY,
                    geom GEOMETRY(Point, 4326)
                );
                CREATE INDEX IF NOT EXISTS vertices_geom_idx ON vertices USING GIST (geom);
            """))
            
            # Edges table
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS edges (
                    id SERIAL PRIMARY KEY,
                    source INTEGER REFERENCES vertices(id),
                    target INTEGER REFERENCES vertices(id),
                    cost DOUBLE PRECISION,
                    geom GEOMETRY(LineString, 4326)
                );
                CREATE INDEX IF NOT EXISTS edges_geom_idx ON edges USING GIST (geom);
                CREATE INDEX IF NOT EXISTS edges_source_idx ON edges (source);
                CREATE INDEX IF NOT EXISTS edges_target_idx ON edges (target);
            """))
            
            cursor.close()
            print("Geospatial routing tables created.")
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise
    
    def drop_tables(self):
        """
        Drop the edges and vertices tables if they exist.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS edges;")
            cursor.execute("DROP TABLE IF EXISTS vertices;")
            cursor.close()
            print("Geospatial routing tables dropped.")
        except Exception as e:
            print(f"Error dropping tables: {e}")
            raise
    
    def add_node(self, node_id, lon, lat):
        """
        Add a node (vertex) to the table.
        
        :param node_id: Unique integer ID for the node.
        :param lon: Longitude (float).
        :param lat: Latitude (float).
        """
        try:
            cursor = self.conn.cursor()
            insert_query = sql.SQL("""
                INSERT INTO vertices (id, geom) 
                VALUES ({id}, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326))
                ON CONFLICT (id) DO NOTHING;
            """).format(
                id=sql.Literal(node_id),
                lon=sql.Literal(lon),
                lat=sql.Literal(lat)
            )
            cursor.execute(insert_query)
            cursor.close()
            print(f"Node {node_id} added at ({lon}, {lat}).")
        except Exception as e:
            print(f"Error adding node: {e}")
            raise
    
    def add_edge(self, source_id, target_id, cost, source_lon=None, source_lat=None, target_lon=None, target_lat=None):
        """
        Add an edge (road segment) between two nodes.
        If source/target coords not provided, queries them from vertices.
        
        :param source_id: Source node ID.
        :param target_id: Target node ID.
        :param cost: Travel cost (e.g., distance or time, float).
        :param source_lon: Optional source longitude (used if nodes not yet inserted).
        :param source_lat: Optional source latitude.
        :param target_lon: Optional target longitude.
        :param target_lat: Optional target latitude.
        """
        try:
            cursor = self.conn.cursor()
            
            # Get or use provided coords
            if source_lon is None or source_lat is None:
                cursor.execute("SELECT ST_X(geom), ST_Y(geom) FROM vertices WHERE id = %s;", (source_id,))
                s_row = cursor.fetchone()
                if s_row is None:
                    raise ValueError(f"Source node {source_id} not found.")
                source_lon, source_lat = s_row
            if target_lon is None or target_lat is None:
                cursor.execute("SELECT ST_X(geom), ST_Y(geom) FROM vertices WHERE id = %s;", (target_id,))
                t_row = cursor.fetchone()
                if t_row is None:
                    raise ValueError(f"Target node {target_id} not found.")
                target_lon, target_lat = t_row
            
            # Create linestring
            insert_query = sql.SQL("""
                INSERT INTO edges (source, target, cost, geom)
                VALUES ({source}, {target}, {cost}, 
                        ST_SetSRID(ST_MakeLine(
                            ST_MakePoint({s_lon}, {s_lat}),
                            ST_MakePoint({t_lon}, {t_lat})
                        ), 4326));
            """).format(
                source=sql.Literal(source_id),
                target=sql.Literal(target_id),
                cost=sql.Literal(cost),
                s_lon=sql.Literal(source_lon),
                s_lat=sql.Literal(source_lat),
                t_lon=sql.Literal(target_lon),
                t_lat=sql.Literal(target_lat)
            )
            cursor.execute(insert_query)
            cursor.close()
            print(f"Edge from {source_id} to {target_id} added with cost {cost}.")
        except Exception as e:
            print(f"Error adding edge: {e}")
            raise
    
    def dijkstra_route(self, start_id, end_id):
        """
        Compute shortest path using Dijkstra algorithm.
        
        :param start_id: Starting node ID.
        :param end_id: Ending node ID.
        :return: List of dicts with seq, node, edge, cost, agg_cost, geom (if available).
        """
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            select_query = sql.SQL("""
                SELECT seq, node, edge, d.cost, d.agg_cost,
                       ST_AsText(e.geom) as geom
                FROM pgr_dijkstra(
                    'SELECT id, source, target, cost FROM edges',
                    {start}, {end}, false
                ) AS d
                LEFT JOIN edges e ON d.edge = e.id
                ORDER BY seq;
            """).format(
                start=sql.Literal(start_id),
                end=sql.Literal(end_id)
            )
            cursor.execute(select_query)
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in Dijkstra route: {e}")
            raise
    
    def astar_route(self, start_id, end_id):
        """
        Compute shortest path using A* algorithm (heuristic: Euclidean distance).
        
        :param start_id: Starting node ID.
        :param end_id: Ending node ID.
        :return: List of dicts with seq, node, edge, cost, agg_cost, geom.
        """
        try:
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            select_query = sql.SQL("""
                SELECT seq, node, edge, a.cost, a.agg_cost,
                       ST_AsText(e.geom) as geom
                FROM pgr_aStar(
                    'SELECT id, source, target, cost, x1, y1, x2, y2 FROM 
                     (SELECT edges.id, edges.source, edges.target, edges.cost,
                             ST_X(v1.geom) as x1, ST_Y(v1.geom) as y1,
                             ST_X(v2.geom) as x2, ST_Y(v2.geom) as y2
                      FROM edges
                      LEFT JOIN vertices v1 ON edges.source = v1.id
                      LEFT JOIN vertices v2 ON edges.target = v2.id) AS e',
                    {start}, {end}, false
                ) AS a
                LEFT JOIN edges e ON a.edge = e.id
                ORDER BY seq;
            """).format(
                start=sql.Literal(start_id),
                end=sql.Literal(end_id)
            )
            cursor.execute(select_query)
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in A* route: {e}")
            raise
    
    def points_in_bbox(self, min_lon, min_lat, max_lon, max_lat, k=10):
        """
        Find up to k nodes within a bounding box.
        
        :param min_lon: Minimum longitude.
        :param min_lat: Minimum latitude.
        :param max_lon: Maximum longitude.
        :param max_lat: Maximum latitude.
        :param k: Limit results (default 10).
        :return: List of (id, lon, lat) tuples.
        """
        try:
            cursor = self.conn.cursor()
            select_query = sql.SQL("""
                SELECT id, ST_X(geom) as lon, ST_Y(geom) as lat
                FROM vertices
                WHERE geom && ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326)
                LIMIT {k};
            """).format(
                min_lon=sql.Literal(min_lon),
                min_lat=sql.Literal(min_lat),
                max_lon=sql.Literal(max_lon),
                max_lat=sql.Literal(max_lat),
                k=sql.Literal(k)
            )
            cursor.execute(select_query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in bbox query: {e}")
            raise
    
    def buffer_around_point(self, lon, lat, radius_meters, return_nodes=True):
        """
        Create a buffer around a point and optionally return nodes within it.
        Radius in meters; uses approximate conversion for SRID 4326.
        
        :param lon: Center longitude.
        :param lat: Center latitude.
        :param radius_meters: Buffer radius in meters.
        :param return_nodes: If True, return nodes inside buffer; else return buffer geom as WKT.
        :return: List of nodes or single buffer WKT string.
        """
        try:
            cursor = self.conn.cursor()
            # Approximate degrees for meters (varies by lat, but simple for test)
            radius_deg = radius_meters / 111000.0  # Rough conversion
            if return_nodes:
                select_query = sql.SQL("""
                    SELECT id, ST_X(geom) as lon, ST_Y(geom) as lat
                    FROM vertices
                    WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326), {radius_deg});
                """).format(
                    lon=sql.Literal(lon),
                    lat=sql.Literal(lat),
                    radius_deg=sql.Literal(radius_deg)
                )
                cursor.execute(select_query)
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                select_query = sql.SQL("""
                    SELECT ST_AsText(ST_Buffer(ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326), {radius_deg}));
                """).format(
                    lon=sql.Literal(lon),
                    lat=sql.Literal(lat),
                    radius_deg=sql.Literal(radius_deg)
                )
                cursor.execute(select_query)
                result = cursor.fetchone()[0]
                cursor.close()
                return result
        except Exception as e:
            print(f"Error in buffer query: {e}")
            raise
    
    def print_network(self):
        """
        Print summary of nodes and edges.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vertices;")
            num_nodes = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM edges;")
            num_edges = cursor.fetchone()[0]
            cursor.close()
            print(f"Network: {num_nodes} nodes, {num_edges} edges.")
        except Exception as e:
            print(f"Error printing network: {e}")
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    import os
    password = os.getenv("POSTGRES_PASSWORD")
    if password is None:
        raise ValueError("POSTGRES_PASSWORD environment variable is not set")
    # Test the PostgresGeoRoutingDB class
    # Adjust connection params if needed
    gdb = PostgresGeoRoutingDB(
        host='localhost',
        port=5433,
        dbname='knowledge_base',
        user='gedgar',
        password=password  # Change to your password
    )
    
    # Create tables
    gdb.create_tables()
    
    # Add sample nodes (simple grid around origin)
    nodes = [
        (1, 0.0, 0.0),    # Node 1
        (2, 1.0, 0.0),    # Node 2
        (3, 0.0, 1.0),    # Node 3
        (4, 1.0, 1.0),    # Node 4
        (5, 2.0, 1.0)     # Node 5
    ]
    for node_id, lon, lat in nodes:
        gdb.add_node(node_id, lon, lat)
    
    # Add sample edges with costs (Euclidean distance approx)
    edges = [
        (1, 2, 1.0),  # 1->2 cost 1
        (1, 3, 1.0),  # 1->3 cost 1
        (2, 4, 1.0),  # 2->4 cost 1
        (3, 4, 1.414),# 3->4 cost sqrt(2)
        (4, 5, 1.0)   # 4->5 cost 1
    ]
    for source, target, cost in edges:
        gdb.add_edge(source, target, cost)
    
    print("\nSample Network Summary:")
    gdb.print_network()
    
    # Test Dijkstra route from 1 to 5
    print("\n--- Dijkstra Route from Node 1 to 5 ---")
    dijkstra_path = gdb.dijkstra_route(1, 5)
    for step in dijkstra_path:
        print(f"Seq {step['seq']}: Node {step['node']}, Edge {step['edge']}, Cost {step['cost']:.3f}, Agg Cost {step['agg_cost']:.3f}, Geom: {step['geom']}")
    
    # Test A* route from 1 to 5
    print("\n--- A* Route from Node 1 to 5 ---")
    astar_path = gdb.astar_route(1, 5)
    for step in astar_path:
        print(f"Seq {step['seq']}: Node {step['node']}, Edge {step['edge']}, Cost {step['cost']:.3f}, Agg Cost {step['agg_cost']:.3f}, Geom: {step['geom']}")
    
    # Test Geo Area: Points in bounding box (around origin)
    print("\n--- Points in Bounding Box (min_lon=-0.5, min_lat=-0.5, max_lon=1.5, max_lat=1.5) ---")
    bbox_points = gdb.points_in_bbox(-0.5, -0.5, 1.5, 1.5)
    for pid, lon, lat in bbox_points:
        print(f"Node {pid}: ({lon}, {lat})")
    
    # Test Buffer: Nodes within 100m buffer around (0.5, 0.5)
    print("\n--- Nodes in 100m Buffer around (0.5, 0.5) ---")
    buffer_nodes = gdb.buffer_around_point(0.5, 0.5, 100)
    for pid, lon, lat in buffer_nodes:
        print(f"Node {pid}: ({lon}, {lat})")
    
    # Drop tables
    print("\n--- Dropping Tables ---")
    gdb.drop_tables()
    
    # Close connection
    gdb.close()
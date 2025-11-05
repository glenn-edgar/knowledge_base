import psycopg2
from psycopg2 import sql
import psycopg2.extras
import numpy as np

class PostgresVectorDB:
    """
    A class to manage a vector table in PostgreSQL using the pgvector extension.
    Assumes the 'vector' extension is already installed in the database.
    Supports major vector patterns: storage, L2 distance search, cosine similarity,
    inner product search, and basic CRUD operations.
    Includes methods to create/ensure the database exists.
    """
    
    def __init__(self, host='localhost', port=5432, dbname='postgres', user='postgres', password='password', table_name='vectors'):
        """
        Initialize the connection to PostgreSQL.
        
        :param host: Database host (default: localhost for container).
        :param port: Database port (default: 5432).
        :param dbname: Database name.
        :param user: Database user.
        :param password: Database password.
        :param table_name: Name of the vector table.
        """
        print("conn_params", host, port, dbname, user, password, table_name)
        self.conn_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self.table_name = table_name
        self.vector_dim = None  # Will be set on table creation
        self.conn = None
        self.ensure_database_exists()
        self.connect()
    
    def ensure_database_exists(self):
        """
        Ensure the specified database exists; create it if it doesn't.
        Connects to the 'postgres' database for this operation.
        """
        try:
            # Connect to the default 'postgres' database to check/create the target DB
            admin_conn_params = self.conn_params.copy()
            admin_conn_params['dbname'] = 'postgres'
            admin_conn = psycopg2.connect(**admin_conn_params)
            admin_conn.autocommit = True
            admin_cur = admin_conn.cursor()
            
            # Check if the target database exists
            admin_cur.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (self.conn_params['dbname'],)
            )
            exists = admin_cur.fetchone()
            
            if not exists:
                admin_cur.execute(
                    sql.SQL("CREATE DATABASE {dbname};").format(
                        dbname=sql.Identifier(self.conn_params['dbname'])
                    )
                )
                print(f"Database '{self.conn_params['dbname']}' created.")
            else:
                print(f"Database '{self.conn_params['dbname']}' already exists.")
            
            admin_cur.close()
            admin_conn.close()
        except Exception as e:
            print(f"Error ensuring database exists: {e}")
            raise
    
    def connect(self):
        """Establish connection to the database."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.conn.autocommit = True
            cursor = self.conn.cursor()
            # Ensure pgvector extension is available
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.close()
            print("Connected to PostgreSQL with pgvector extension.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def create_table(self, dim=3):
        """
        Create the vector table if it does not exist.
        
        :param dim: Dimension of the vectors (fixed for the column).
        """
        try:
            cursor = self.conn.cursor()
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    embedding VECTOR({dim})
                );
            """).format(
                table=sql.Identifier(self.table_name),
                dim=sql.Literal(dim)
            )
            cursor.execute(create_table_query)
            self.vector_dim = dim
            cursor.close()
            print(f"Vector table '{self.table_name}' created with dimension {dim}.")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise
    
    def drop_table(self):
        """
        Drop the vector table if it exists.
        """
        try:
            cursor = self.conn.cursor()
            drop_table_query = sql.SQL("""
                DROP TABLE IF EXISTS {table};
            """).format(
                table=sql.Identifier(self.table_name)
            )
            cursor.execute(drop_table_query)
            cursor.close()
            print(f"Vector table '{self.table_name}' dropped.")
        except Exception as e:
            print(f"Error dropping table: {e}")
            raise
    
    def add_vector(self, embedding, id=None):
        """
        Add a single vector to the table.
        
        :param embedding: List or numpy array of floats (length must match dim).
        :param id: Optional explicit ID.
        """
        if len(embedding) != self.vector_dim:
            raise ValueError(f"Embedding dimension must be {self.vector_dim}.")
        embedding_str = '[' + ','.join(map(str, embedding)) + ']'
        try:
            cursor = self.conn.cursor()
            if id is None:
                insert_query = sql.SQL("""
                    INSERT INTO {table} (embedding) VALUES ({embedding});
                """).format(
                    table=sql.Identifier(self.table_name),
                    embedding=sql.Literal(embedding_str)
                )
            else:
                insert_query = sql.SQL("""
                    INSERT INTO {table} (id, embedding) VALUES ({id}, {embedding});
                """).format(
                    table=sql.Identifier(self.table_name),
                    id=sql.Literal(id),
                    embedding=sql.Literal(embedding_str)
                )
            cursor.execute(insert_query)
            cursor.close()
            print(f"Vector added: {embedding}")
        except Exception as e:
            print(f"Error adding vector: {e}")
            raise
    
    def add_vectors(self, embeddings):
        """
        Add multiple vectors to the table.
        
        :param embeddings: List of lists or numpy arrays.
        """
        for emb in embeddings:
            self.add_vector(emb)
    
    def get_all_vectors(self):
        """
        Retrieve all vectors from the table.
        
        :return: List of (id, embedding) tuples.
        """
        try:
            cursor = self.conn.cursor()
            select_query = sql.SQL("""
                SELECT id, embedding FROM {table};
            """).format(
                table=sql.Identifier(self.table_name)
            )
            cursor.execute(select_query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error retrieving vectors: {e}")
            raise
    
    def nearest_neighbors_l2(self, query_embedding, k=5):
        """
        Find k nearest neighbors using L2 (Euclidean) distance.
        
        :param query_embedding: Query vector (list or numpy array).
        :param k: Number of neighbors to return.
        :return: List of (id, embedding, distance) tuples.
        """
        if len(query_embedding) != self.vector_dim:
            raise ValueError(f"Query dimension must be {self.vector_dim}.")
        query_str = '[' + ','.join(map(str, query_embedding)) + ']'
        try:
            cursor = self.conn.cursor()
            select_query = sql.SQL("""
                SELECT id, embedding, embedding <-> {query} AS distance
                FROM {table}
                ORDER BY distance ASC
                LIMIT {k};
            """).format(
                table=sql.Identifier(self.table_name),
                query=sql.Literal(query_str),
                k=sql.Literal(k)
            )
            cursor.execute(select_query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in L2 search: {e}")
            raise
    
    def cosine_similarity_search(self, query_embedding, k=5):
        """
        Find k most similar vectors using cosine similarity (1 - cosine distance).
        
        :param query_embedding: Query vector.
        :param k: Number of results.
        :return: List of (id, embedding, similarity) tuples, where similarity is cosine sim.
        """
        if len(query_embedding) != self.vector_dim:
            raise ValueError(f"Query dimension must be {self.vector_dim}.")
        # pgvector cosine distance is 1 - cos_sim, so order by that ASC for highest sim
        query_str = '[' + ','.join(map(str, query_embedding)) + ']'
        try:
            cursor = self.conn.cursor()
            select_query = sql.SQL("""
                SELECT id, embedding, 1 - (embedding <=> {query}) AS similarity
                FROM {table}
                ORDER BY embedding <=> {query} ASC
                LIMIT {k};
            """).format(
                table=sql.Identifier(self.table_name),
                query=sql.Literal(query_str),
                k=sql.Literal(k)
            )
            cursor.execute(select_query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in cosine similarity search: {e}")
            raise
    
    def inner_product_search(self, query_embedding, k=5):
        """
        Find k vectors with highest inner product (order by negative inner product ASC).
        
        :param query_embedding: Query vector.
        :param k: Number of results.
        :return: List of (id, embedding, inner_product) tuples.
        """
        if len(query_embedding) != self.vector_dim:
            raise ValueError(f"Query dimension must be {self.vector_dim}.")
        query_str = '[' + ','.join(map(str, query_embedding)) + ']'
        try:
            cursor = self.conn.cursor()
            select_query = sql.SQL("""
                SELECT id, embedding, {query} <#> embedding AS neg_ip
                FROM {table}
                ORDER BY neg_ip ASC
                LIMIT {k};
            """).format(
                table=sql.Identifier(self.table_name),
                query=sql.Literal(query_str),
                k=sql.Literal(k)
            )
            cursor.execute(select_query)
            results = [(row[0], row[1], -row[2]) for row in cursor.fetchall()]  # Convert back to positive IP
            cursor.close()
            return results
        except Exception as e:
            print(f"Error in inner product search: {e}")
            raise
    
    def print_table(self):
        """
        Print all vectors in the table.
        """
        vectors = self.get_all_vectors()
        if not vectors:
            print("Vector table is empty.")
            return
        print("Vector Table:")
        for vid, emb in vectors:
            print(f"ID {vid}: {emb}")
    
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
    # Test the PostgresVectorDB class
    # Adjust connection params if needed for your container setup
    vt = PostgresVectorDB(
        host='localhost',
        port=5432,
        dbname='knowledge_base',  # Using a test database name
        user='gedgar',
        password=password,  # Change to your container's password
        table_name='test_vectors'
    )
    
    # Create table with 3D vectors
    vt.create_table(dim=3)
    
    # Add sample 3D vectors
    sample_vectors = [
        [1.0, 2.0, 3.0],  # Vector 1
        [4.0, 5.0, 6.0],  # Vector 2
        [7.0, 8.0, 9.0],  # Vector 3
        [1.0, 1.0, 1.0],  # Vector 4 (close to first)
        [10.0, 0.0, 0.0]  # Vector 5
    ]
    vt.add_vectors(sample_vectors)
    
    print("\nInitial Vector Table:")
    vt.print_table()
    
    # Perform vector searches and print results
    print("\n--- Vector Operations (Searches) ---")
    
    # L2 Nearest Neighbors
    query_l2 = [1.0, 2.0, 3.0]
    nn_l2 = vt.nearest_neighbors_l2(query_l2, k=3)
    print(f"L2 Nearest Neighbors to {query_l2}:")
    for vid, emb, dist in nn_l2:
        print(f"  ID {vid}: {emb} (distance: {dist:.4f})")
    
    # Cosine Similarity Search
    query_cos = [1.0, 1.0, 1.0]
    sim_cos = vt.cosine_similarity_search(query_cos, k=3)
    print(f"Cosine Similarity to {query_cos}:")
    for vid, emb, sim in sim_cos:
        print(f"  ID {vid}: {emb} (similarity: {sim:.4f})")
    
    # Inner Product Search
    query_ip = [1.0, 2.0, 3.0]
    ip_results = vt.inner_product_search(query_ip, k=3)
    print(f"Highest Inner Products with {query_ip}:")
    for vid, emb, ip in ip_results:
        print(f"  ID {vid}: {emb} (inner product: {ip:.4f})")
    
    # Drop the table
    print("\n--- Dropping Table ---")
    vt.drop_table()
    
    # Close connection
    vt.close()
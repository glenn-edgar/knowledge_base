   CREATE SCHEMA IF NOT EXISTS stream_table;
            CREATE TABLE stream_table.job_table(
            
                path LTREE UNIQUE
                recorded_at TIMESTAMP DEFAULT NOW(),
               
                data JSON,
                
            );
        """)

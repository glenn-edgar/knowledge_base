     CREATE SCHEMA IF NOT EXISTS job_table;
            CREATE TABLE status_table.job_table(
                id SERIAL PRIMARY KEY,
               
                path LTREE UNIQUE
                schedule_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT FALSE,
                data JSON,
                
            );
        """)

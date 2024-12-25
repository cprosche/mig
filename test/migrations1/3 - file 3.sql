-- up
CREATE TABLE test_table_3 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
-- down
DROP TABLE test_table_3;
CREATE EXTENSION IF NOT EXISTS citus;

SELECT citus_set_coordinator_host('coordinator', 5432);

SELECT * FROM citus_add_node('worker1', 5432);
SELECT * FROM citus_add_node('worker2', 5432);
SELECT * FROM citus_add_node('worker3', 5432);

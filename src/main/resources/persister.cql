# create cassandra keyspace
CREATE KEYSPACE if not exists development WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

# create signals table
CREATE TABLE if not exists development.signals (
 id bigint, timestamp timestamp, value int, producerId text, PRIMARY KEY(id, timestamp)
) WITH CLUSTERING ORDER BY(timestamp desc);
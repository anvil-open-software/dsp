# create cassandra keyspace
CREATE KEYSPACE development if not exists WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

# create signals table
CREATE TABLE if not exists development.signals (
 id long, timestamp timestamp, value int, generatorId text, PRIMARY KEY(id, timestamp)
) WITH CLUSTERING ORDER BY(timestamp desc);
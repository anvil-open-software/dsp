// create signals table
CREATE TABLE if not exists signals (id bigint, timestamp timestamp, value int, producerId text, PRIMARY KEY(id, timestamp)) WITH CLUSTERING ORDER BY(timestamp desc);
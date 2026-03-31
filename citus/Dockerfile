FROM citusdata/citus:postgres_16

RUN apt-get update && apt-get install -y nano && rm -rf /var/lib/apt/lists/*

# change wal_level for allowing resharding
RUN echo "wal_level = logical" >> /usr/share/postgresql/postgresql.conf.sample

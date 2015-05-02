from datetime import timedelta, datetime, date


class ReadConf(object):
    def __init__(self, sc, split_size=None, fetch_size=None, consistency_level=None, metrics_enabled=None):
        self.jvm = sc._jvm
        
        self.split_size = split_size
        self.fetch_size = fetch_size
        self.consistency_level = consistency_level
        self.metrics_enabled = metrics_enabled
        
    def to_java_conf(self):
        ''' Create the com.datastax.spark.connector.rdd.ReadConf JVM object'''
        
        split_size = self.split_size or self.jvm.ReadConf.DefaultSplitSize()
        fetch_size = self.fetch_size or self.jvm.ReadConf.DefaultFetchSize()
        consistency_level = self.jvm.ConsistencyLevel.values()[self.consistency_level] \
            if self.consistency_level else self.jvm.ReadConf.DefaultConsistencyLevel()
        # TODO metrics_enabled = jvm.ReadConf.DefaultReadTaskMetricsEnabled() \
        #    if metrics_enabled is None else metrics_enabled
        metrics_enabled = False if self.metrics_enabled is None else self.metrics_enabled
                
        return self.jvm.ReadConf(
            split_size,
            fetch_size,
            consistency_level,
            metrics_enabled,
        )
        
    

class WriteConf(object):
    def __init__(self, sc, batch_size=None, batch_buffer_size=None, batch_grouping_key=None, consistency_level=None,
                 parallelism_level=None, throughput_mibps=None, ttl=None, timestamp=None, metrics_enabled=None):
        '''
            @param sc(SparkContext):
                The spark context used to build the WriteConf object
            @param batch_size(int):
                The size in bytes to batch up in an unlogged batch of CQL inserts.
                If None given the default size of 16*1024 is used or spark.cassandra.output.batch.size.bytes if set.
            @param batch_buffer_size(int):
                The maximum number of batches which are 'pending'.
                If None given the default of 1000 is used.
            @param batch_grouping_key(string):
                The way batches are formed:
                * all: any row can be added to any batch
                * replicaset: rows are batched for replica sets 
                * partition: rows are batched by their partition key
                * None: defaults to "partition"
            @param consistency_level(cassandra.ConsistencyLevel):
                The consistency level used in writing to Cassandra.
                If None defaults to LOCAL_ONE or spark.cassandra.output.consistency.level if set.
            @param parallelism_level(int):
                The maximum number of batches written in parallel.
                If None defaults to 8 or spark.cassandra.output.concurrent.writes if set.
            @param throughput_mibps(int):
            @param ttl(int or timedelta):
                The time to live as milliseconds or timedelta to use for the values.
                If None given no TTL is used.
            @param timestamp(int, date or datetime):
                The timestamp in milliseconds, date or datetime to use for the values.
                If None given the Cassandra nodes determine the timestamp.
            @param metrics_enabled(bool):
                Whether to enable task metrics updates.
        '''
        self.batch_size = batch_size
        self.batch_buffer_size = batch_buffer_size
        self.batch_grouping_key = batch_grouping_key
        self.consistency_level = consistency_level
        self.parallelism_level = parallelism_level
        self.throughput_mibps = throughput_mibps

        # convert time delta in ttl in seconds        
        if ttl and isinstance(ttl, timedelta):
            ttl = int(self.ttl.total_seconds())
        self.ttl = ttl
        
        # convert date or datetime objects to a timestamp in milliseconds since the UNIX epoch
        if timestamp and (isinstance(timestamp, datetime) or isinstance(timestamp, date)):
            timestamp = int((timestamp - timestamp.__class__(1970, 1, 1)).total_seconds() * 1000)
        self.timestamp = timestamp
        
        self.metrics_enabled = metrics_enabled

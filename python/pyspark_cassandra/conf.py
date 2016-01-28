# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta, datetime, date


class _Conf(object):
    @classmethod
    def build(cls, conf=None, **kwargs):
        if conf and kwargs:
            settings = conf.settings()
            settings.update(kwargs)
            return cls(**settings)
        elif conf:
            return conf
        else:
            return cls(**kwargs)

    def settings(self):
        return {k:v for k, v in self.__dict__.items() if v is not None}

    def __str__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join('%s=%s' % (k, v) for k, v in self.settings().items())
        )


class ReadConf(_Conf):
    def __init__(self, split_count=None, split_size=None, fetch_size=None, consistency_level=None,
                 metrics_enabled=None):
        '''
            TODO docstring
        '''
        self.split_count = split_count
        self.split_size = split_size
        self.fetch_size = fetch_size
        self.consistency_level = consistency_level
        self.metrics_enabled = metrics_enabled


class WriteConf(_Conf):
    def __init__(self, batch_size=None, batch_buffer_size=None, batch_grouping_key=None,
                 consistency_level=None, parallelism_level=None, throughput_mibps=None, ttl=None,
                 timestamp=None, metrics_enabled=None):
        '''
            @param batch_size(int):
                The size in bytes to batch up in an unlogged batch of CQL inserts.
                If None given the default size of 16*1024 is used or
                spark.cassandra.output.batch.size.bytes if set.
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
                The time to live as seconds or timedelta to use for the values.
                If None given no TTL is used.
            @param timestamp(int, date or datetime):
                The timestamp in microseconds, date or datetime to use for the values.
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
            ttl = int(ttl.total_seconds())
        self.ttl = ttl

        # convert date or datetime objects to a timestamp in milliseconds since the UNIX epoch
        if timestamp and (isinstance(timestamp, datetime) or isinstance(timestamp, date)):
            timestamp = (timestamp - timestamp.__class__(1970, 1, 1)).total_seconds()
            timestamp = int(timestamp * 1000 * 1000)
        self.timestamp = timestamp

        self.metrics_enabled = metrics_enabled

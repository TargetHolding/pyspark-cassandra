class RowFormat(object):
    """An enumeration of CQL row formats used in Cassandra RDD's"""
    
    DICT = 0
    TUPLE = 1
    ROW = 2
    
    values = (DICT, TUPLE, ROW)
    
    def __init__(self):
        raise NotImplemented('RowFormat is not meant to be initialized, use e.g. RowFormat.DICT')


class ColumnSelector():
    def __init__(self, partition_key=False, primary_key=False, *columns):
        if sum([bool(partition_key), bool(primary_key), bool(columns)]) > 1:
            raise ValueError("can't combine selection of partition_key and/or primar_key and/or columns")
        
        self.partition_key = partition_key
        self.primary_key = primary_key
        self.columns = columns
        
    @classmethod
    def none(cls):
        return ColumnSelector()
    
    @classmethod
    def partition_key(cls):
        return ColumnSelector(partition_key=True)
        
    @classmethod
    def primary_key(cls):
        return ColumnSelector(primary_key=True)
        
    @classmethod
    def some(cls, *columns):
        return ColumnSelector(columns)
    
    def __str__(self):
        s = '[column selection of: '
        if self.partition_key:
            s += 'partition_key'
        elif self.primary_key:
            s += 'primary_key'
        elif self.columns:
            s += ', '.join(c for c in self.columns)
        else:
            s += 'nothing'
        return s + ']'

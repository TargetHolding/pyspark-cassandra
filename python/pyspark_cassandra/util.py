_helper = None

def helper(ctx):
    global _helper
    
    if not _helper:
        _helper = ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass("pyspark_cassandra.PythonHelper").newInstance()
    
    return _helper

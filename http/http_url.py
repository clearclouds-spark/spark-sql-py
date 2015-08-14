from __future__ import print_function

import os
import sys
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files.
    if len(sys.argv) < 2:
        path = "file://" + os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    df = sqlContext.read.json(path).cache()
    total_groups = df.select("dst_group_id").distinct().dropna().collect()
   
    # Register this DataFrame as a table.
    df.registerAsTable("http")

    urls = sqlContext.sql("SELECT dst_group_id, url, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as in_bytes, sum(out_bytes) as out_bytes, sum(latency_sec * 1000000 + latency_usec) as latency, count(*) as requests FROM http where url is not null group by dst_group_id, url")
    
    urls.registerAsTable("urls")
    output = {}
    for opt in ['total_bytes', 'in_bytes', 'out_bytes', 'latency', 'requests']:
        output[opt] = {}
        for group_id in total_groups:
            topN_sql = "select url, total_bytes, in_bytes, out_bytes, latency, requests from urls where dst_group_id = %s order by %s desc limit 50" % (group_id.dst_group_id, opt)
            topN_collect = sqlContext.sql(topN_sql).toJSON().collect()
            output[opt][group_id.dst_group_id] = list(json.loads(x) for x in topN_collect) 
    
    with open("result.json", "w") as f:
        json.dump(output, f, indent=4)
    sc.stop()


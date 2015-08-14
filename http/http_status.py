import os
import sys
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, when 

if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)
    if len(sys.argv) < 2:
        path = "file://" + os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    df = sqlContext.read.json(path).cache()
    status_info = df.groupby("dst_group_id", "status_code").count().toJSON().collect()
    output = {}
    
    for s in status_info:
        d_s = json.loads(s)
        k = d_s['dst_group_id']
        if k not in output.keys(): 
            output[k] = {}
        output[k][d_s['status_code']] = d_s['count']
    
    with open("http_status_result.json", "w") as f:
        json.dump(output, f, indent=4)
    sc.stop()

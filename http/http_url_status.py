import os
import sys
import json
import urllib
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import udf 


def get_url_status(x):
    d_x = json.loads(x)
    d_x[d_x['status']] = d_x['count']
    for u in status_info.toJSON().collect():
        d_u = json.loads(u)
        if d_x['dst_group_id'] == d_u['dst_group_id'] and d_x['url'] == d_u['url'] and d_x['status'] != d_u['status']:
            d_x[d_u['status']] = d_u['count']
    d_x.pop('status')
    d_x.pop('dst_group_id')
    d_x.pop('count')
    return d_x


if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)
    if len(sys.argv) < 2:
        path = "file://" + os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    df = sqlContext.read.json(path).cache()
    total_groups = df.select("dst_group_id").distinct().dropna().collect()

    status_key = udf(lambda x: str(int(x)/100)+"xx")
    status_info = df.groupby("dst_group_id", "url", status_key(df.status_code).alias("status")).count()

    output = {}
    for opt in ['1xx', '2xx', '3xx', '4xx', '5xx']:
        output[opt] = {}
        for group_id in total_groups:
            output[opt][group_id.dst_group_id] = list(get_url_status(x) for x in status_info.sort(status_info['count'].desc()).limit(50).toJSON().collect() if json.loads(x)['status'] == opt)
 
    with open("http_url_status.json", "w") as f:
        json.dump(output, f, indent=4)
    sc.stop()

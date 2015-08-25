import sys
import os
import json
import urllib
import copy
from datetime import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def dump_file(name, output, path, timestamp):
    base_path = "/home/juyun/datafile/json-file/live/peer1/"
    #path = base_path + datetime.now().strftime("%Y/%m/%d/%H/")
    path = base_path + path
    if not os.path.exists(path):
        mkdir_p(path)
    name = "nfcapd." + timestamp + "." + name
    print(path + name)
    with open(path + name, "w+") as f:
        json.dump(output, f, indent=4)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5 (except OSError, exc: for Python <2.5)
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


def tcp_info_all(tcp, tcp_add):
    if 0 == len(tcp):
        tcp = copy.deepcopy(tcp_add)
    else:
        tcp["bytes_total"]   +=tcp_add["bytes_total"]
        tcp["bytes_in"]      +=tcp_add["bytes_in"]
        tcp["bytes_out"]     +=tcp_add["bytes_out"]
        tcp["packets_total"]  +=tcp_add["packets_total"]
        tcp["packets_in"]    +=tcp_add["packets_in"]
        tcp["packets_out"]   +=tcp_add["packets_out"]
        tcp["retransmitted_in"]   +=tcp_add["retransmitted_in"]
        tcp["retransmitted_out"]  +=tcp_add["retransmitted_out"]
        tcp["ooorder_in"]    +=tcp_add["ooorder_in"]            
        tcp["ooorder_out"]   +=tcp_add["ooorder_out"]   
        tcp["server_latency_total"]+=tcp_add["server_latency_total"]
        tcp["client_latency_total"]+=tcp_add["client_latency_total"]
        tcp["counter"] +=tcp_add["counter"]
        tcp["new"]    +=tcp_add["new"]
        tcp["closed"]  +=tcp_add["closed"]
        tcp["fin"]      +=tcp_add["fin"]
        tcp["reset"]   +=tcp_add["reset"]
        tcp["timeout"] +=tcp_add["timeout"]
        tcp["scan"]    +=tcp_add["scan"]
        tcp["syn_attack"]  +=tcp_add["syn_attack"]                      
    return tcp    
        
def process(time, rdd):
    #try:
    print "========= %s =========" % str(time)
    path = time.strftime("%Y/%m/%d/%H/")
    timestamp = time.strftime("%Y%m%d%H%M%S")
    if rdd.count() > 0:
        sqlContext = getSqlContextInstance(rdd.context)
        lines = rdd.map(lambda x: x[1])
        df = sqlContext.jsonRDD(lines)
        #df.printSchema()
        total_groups = df.select("dst_group_id").distinct().dropna().collect()
        total_l7_protos = df.select("l7_proto").distinct().dropna().collect()
        total_dst_ports = df.select("dst_port").distinct().dropna().collect()
        df.registerTempTable("tcp")

        ### process tcp proto  ###
        context = sqlContext.sql("SELECT dst_group_id, l7_proto, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as bytes_in, "
                + "sum(out_bytes) as bytes_out FROM tcp group by dst_group_id, l7_proto")
        context = context.toJSON().collect()
        output = {}
        output["all"] = {}
        
        for opt in context:
            tmp = json.loads(opt)
            group_id = tmp["dst_group_id"]
            proto_id = tmp["l7_proto"]
            if group_id not in output:
                output[group_id] = {}
            output[group_id][proto_id] = tmp
        
        context = sqlContext.sql("SELECT l7_proto, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as bytes_in, "
                + "sum(out_bytes) as bytes_out FROM tcp group by l7_proto")
        context = context.toJSON().collect()
        for opt in context:
            tmp =  json.loads(opt)
            proto_id = tmp["l7_proto"] 
            output["all"][proto_id] = tmp

        #print(output)
        dump_file("tcp_proto_10s", output, path, timestamp)


        ### process tcp dst_port ###

        context = sqlContext.sql("SELECT dst_group_id, dst_port as port, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as bytes_in, "
                    + "sum(out_bytes) as bytes_out FROM tcp group by dst_group_id, dst_port")
        #context.show()
        context = context.toJSON().collect()
        output = {}
        output["all"] = {}
        for opt in context:
            tmp =  json.loads(opt)
            group_id = tmp["dst_group_id"]
            dst_port = tmp["port"] 
            if group_id not in output:
                output[group_id] = {}
            output[group_id][dst_port] = tmp

        context = sqlContext.sql("SELECT dst_port as port, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as bytes_in, "
                    + "sum(out_bytes) as bytes_out FROM tcp group by dst_port")
        context = context.toJSON().collect()
        for opt in context:
            tmp =  json.loads(opt)
            dst_port = tmp["port"] 
            output["all"][dst_port] = tmp

        dump_file("tcp_dstport_10s", output, path, timestamp)


        ### process tcp info

        tcp_conns = {}
        context = sqlContext.sql("SELECT dst_group_id , final_status, unknown_conn, count(final_status) as flows,"
                    + " sum(client_latency_sec) as client_latency_sec, sum(client_latency_usec) as client_latency_usec, sum(server_latency_sec) as server_latency_sec,"
                    + " sum(server_latency_usec) as server_latency_usec"
                    + " FROM tcp where (final_status != 5 and final_status != 6) group by dst_group_id, unknown_conn, final_status")
        context.show()
        context = context.toJSON().collect()
        for opt in context:
            tmp =  json.loads(opt)
            group_id = tmp["dst_group_id"]
            status = tmp["final_status"]
            unknown_conn = tmp["unknown_conn"]
            flows = tmp["flows"]
            if group_id not in tcp_conns.keys():
                tcp_conns[group_id] = {"server_latency_total":0,"client_latency_total":0,"counter":0,
                                "new":0,"closed":0,"fin":0,"reset":0,"timeout":0,"scan":0,"syn_attack":0}
            tcp_conn_info = tcp_conns[group_id]
            if unknown_conn == 0:
                tcp_conn_info["counter"] += flows
                tcp_conn_info["server_latency_total"] = flows * (tmp["server_latency_sec"] * 1000000 + tmp["server_latency_usec"])
                tcp_conn_info["client_latency_total"] = flows * (tmp["client_latency_sec"] * 1000000 + tmp["client_latency_usec"])
            tcp_conn_info["closed"] += flows
            if status == 1:
                tcp_conn_info["scan"] += flows
            elif status == 2:
                tcp_conn_info["syn_attack"] += flows
            elif status == 7:
                tcp_conn_info["reset"] += flows
            elif status == 8:
                tcp_conn_info["fin"] += flows
            if status in [1, 2, 3, 4]: 
                tcp_conn_info["timeout"] += flows
 
        #print(tcp_conns)
        #return tcp_conns 

        #######

        context = sqlContext.sql("SELECT dst_group_id , sum(in_bytes + out_bytes) as bytes_total, sum(in_bytes) as bytes_in, sum(out_bytes) as bytes_out,"
                    + " sum(in_pkts + out_pkts) as packets_total, sum(in_pkts) as packets_in, sum(out_pkts) as packets_out, sum(retransmitted_in_pkts) as retransmitted_in,"
                    + " sum(retransmitted_out_pkts) as retransmitted_out, sum(ooorder_in_pkts) as ooorder_in, sum(ooorder_out_pkts) as ooorder_out,"
                    + " sum(tcp_window_size_zero) as tcp_window_size_zero FROM tcp group by dst_group_id")
       
        context.show()
        context.printSchema()
        #context.registerTempTable("tcp_info")
        context = context.toJSON().collect()
        
        output = {}
        output["all"] = {}
        for opt in context:
            tmp = json.loads(opt)
            group_id = tmp["dst_group_id"]
            if group_id in tcp_conns.keys():
                tcp_conn_info = tcp_conns[group_id]
            else:
                tcp_conn_info = {"server_latency_total":0, "client_latency_total":0, "counter":0,
                                "new":0, "closed":0, "fin":0, "reset":0, "timeout":0, "scan":0, "syn_attack":0}
 
            tmp["retransfer_rate"] = 0
            tmp["outoforder_rate"] = 0
            tmp["server_latency"] = 0
            tmp["client_latency"] = 0
 
            tmp["new"] = 0
            tmp["counter"] = tcp_conn_info["counter"]
            tmp["server_latency_total"] = tcp_conn_info["server_latency_total"]
            tmp["client_latency_total"] = tcp_conn_info["client_latency_total"]
            tmp["closed"] = tcp_conn_info["closed"]
            tmp["fin"] = tcp_conn_info["fin"]
            tmp["reset"] = tcp_conn_info["reset"]
            tmp["timeout"] = tcp_conn_info["timeout"]
            tmp["scan"] = tcp_conn_info["scan"]
            tmp["syn_attack"] = tcp_conn_info["syn_attack"]
 
            if tmp["counter"] > 1:
                tmp["server_latency"] = tmp["server_latency_total"] / tmp["counter"]
                tmp["client_latency"] = tmp["client_latency_total"] / tmp["counter"]
 
            if tmp["packets_total"] > 1:
                tmp["retransfer_rate"] = (tmp['retransmitted_in'] + tmp['retransmitted_out'])*1.0 / tmp["packets_total"]
                tmp["outoforder_rate"] = (tmp['ooorder_in'] + tmp['ooorder_out'])*1.0 / tmp["packets_total"]
            
            output[group_id] = tmp
            
            output["all"] = tcp_info_all(output["all"], output[group_id])
 
        dump_file("tcp_10s", output, path, timestamp)


        #### process tcp server
        server_conns = {}
        context = sqlContext.sql("SELECT dst_ipv4, final_status, unknown_conn, count(final_status) as flows,"
                    + " sum(client_latency_sec) as client_latency_sec, sum(client_latency_usec) as client_latency_usec, sum(server_latency_sec) as server_latency_sec,"
                    + " sum(server_latency_usec) as server_latency_usec"
                    + " FROM tcp where (final_status != 5 and final_status != 6) group by dst_ipv4, unknown_conn, final_status")
        #context.show()
        context = context.toJSON().collect()
        for opt in context:
            tmp =  json.loads(opt)
            dst_ip = tmp["dst_ipv4"]
            status = tmp["final_status"]
            unknown_conn = tmp["unknown_conn"]
            flows = tmp["flows"]
            if dst_ip not in server_conns.keys():
                server_conns[dst_ip]={"server_latency_total":0,"client_latency_total":0,"counter":0,
                                                       "new":0,"closed":0,
                                                       "fin":0,"reset":0,"timeout":0,"scan":0,"syn_attack":0}
            info = server_conns[dst_ip]
            if unknown_conn == 0:
                info["counter"] += flows
                info["server_latency_total"] = flows * (tmp["server_latency_sec"] * 1000000 + tmp["server_latency_usec"])
                info["client_latency_total"] = flows * (tmp["client_latency_sec"] * 1000000 + tmp["client_latency_usec"])
            info["closed"] += flows
            if status == 1:
                info["scan"] += flows
            elif status == 2:
                info["syn_attack"] += flows
            elif status == 7:
                info["reset"] += flows
            elif status == 8:
                info["fin"] += flows
            if status in [1, 2, 3, 4]: 
                info["timeout"] += flows
 
        #print(server_conns)
        #return server_conns

        #server_conns = tcp_server_conn()
 
        context = sqlContext.sql("SELECT dst_group_id , dst_ipv4, sum(in_bytes + out_bytes) as bytes_total, sum(in_bytes) as bytes_in, sum(out_bytes) as bytes_out,"
                    + " sum(in_pkts + out_pkts) as packets_total, sum(in_pkts) as packets_in, sum(out_pkts) as packets_out, sum(retransmitted_in_pkts) as retransmitted_in,"
                    + " sum(retransmitted_out_pkts) as retransmitted_out, sum(ooorder_in_pkts) as ooorder_in, sum(ooorder_out_pkts) as ooorder_out,"
                    + " sum(tcp_window_size_zero) as tcp_window_size_zero FROM tcp group by dst_group_id, dst_ipv4")
       
        #context.show()
        context = context.toJSON().collect()
        
        output = {}
        output["all"] = {}
        for opt in context:
            tmp = json.loads(opt)
            group_id = tmp["dst_group_id"]
            dst_ip = tmp["dst_ipv4"]
            if group_id not in output:
                output[group_id] = {}
            if dst_ip in server_conns:
                info = server_conns[dst_ip]
            else:
                info = {"server_latency_total":0, "client_latency_total":0, "counter":0,
                                "new":0, "closed":0, "fin":0, "reset":0, "timeout":0, "scan":0, "syn_attack":0}
 
            tmp["retransfer_rate"] = 0
            tmp["outoforder_rate"] = 0
            tmp["server_latency"] = 0
            tmp["client_latency"] = 0
 
            tmp["new"] = 0
            tmp["counter"] = info["counter"]
            tmp["server_latency_total"] = info["server_latency_total"]
            tmp["client_latency_total"] = info["client_latency_total"]
            tmp["closed"] = info["closed"]
            tmp["fin"] = info["fin"]
            tmp["reset"] = info["reset"]
            tmp["timeout"] = info["timeout"]
            tmp["scan"] = info["scan"]
            tmp["syn_attack"] = info["syn_attack"]
 
            if tmp["counter"] > 1:
                tmp["server_latency"] = tmp["server_latency_total"] / tmp["counter"]
                tmp["client_latency"] = tmp["client_latency_total"] / tmp["counter"]
 
            if tmp["packets_total"] > 1:
                tmp["retransfer_rate"] = (tmp['retransmitted_in'] + tmp['retransmitted_out'])*1.0 / tmp["packets_total"]
                tmp["outoforder_rate"] = (tmp['ooorder_in'] + tmp['ooorder_out'])*1.0 / tmp["packets_total"]
            
            output[group_id][dst_ip] = tmp
 
        dump_file("tcp_server_10s", output, path, timestamp)

    #except:
       # print "error"
       # pass


if __name__ == "__main__":
    sc = SparkContext(appName="TcpPythonSQL")
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 10)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()

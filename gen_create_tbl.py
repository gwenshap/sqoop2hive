#!/usr/bin/python
import json
import subprocess
import sys

def convertType(type):
	if type=="long":
		return "bigint"
	else:
		return type

def gen_columns(schema):
	ret = "(" + ",".join(['%s %s' % (field['name'],convertType(field['type'][0])) for (field) in schema['fields']]) + ")"
	return ret
  
def read_schema_file(hdfspath):
	p = subprocess.Popen(['hdfs','dfs','-cat',hdfspath],stdout=subprocess.PIPE)
	return p.communicate()[0]
	
def usage(argNum):
	print ("Wrong number of arguments: " + `argNum`)
	print (sys.argv)
	print("usage: "+args[0]+ " table_name hdfs_data_location hdfs_avro_schema_location [--partitions partitions] ")
	sys.exit(1)


argNum = len(sys.argv)

if (argNum < 4):
	usage(argNum)
elif (argNum == 4):
	table_name = sys.argv[1]
	hdfs_data_location = sys.argv[2]
	hdfs_avro_schema_location = sys.argv[3]
	partitions = " "
elif (argNum==6):
	table_name = sys.argv[1]
        hdfs_data_location = sys.argv[2]
        hdfs_avro_schema_location = sys.argv[3]
	partitions = "partitioned by " + sys.argv[5]
else:
	usage(argNum)	  


#schema_file = file('/tmp/'+args.table_name+'.schema','r')

schema_obj = json.loads(read_schema_file(hdfs_avro_schema_location))





print """CREATE EXTERNAL TABLE %s
%s
%s 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
location '%s' 
TBLPROPERTIES (   'avro.schema.url'='%s')""" % (table_name,gen_columns(schema_obj),partitions,hdfs_data_location,hdfs_avro_schema_location);

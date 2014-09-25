source common_env.sh

[ $# -eq 0 ] && { echo "Usage: $0 table_name num_mappers"; exit 1; }

echo Step 1: Clean existing data
hdfs dfs -rm -r /${top_dir}/${table_name}
hdfs dfs -rm -r /${metadata_dir}/${schema_name}
beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${table_name}"

echo Step2: Sqoop the data from Oracle
sqoop import -D oraoop.disabled=true --connect ${connect_string} --table ${table_name} --verbose --username ${user_name} --password-file ${password_file} --as-avrodatafile --warehouse-dir /${top_dir} 

echo Step 3: Extract AVRO schema and load to HDFS
hadoop jar /opt/cloudera/parcels/CDH/lib/avro/avro-tools.jar getschema hdfs://nameservice1/${top_dir}/${table_name}/part-m-00000.avro > /tmp/${schema_name}
./gen_create_tbl.py ${table_name} hdfs://nameservice1/${top_dir}/${table_name} hdfs://nameservice1/${metadata_dir}/${schema_name} > /tmp/${table_name}.ddl

hdfs dfs -put /tmp/${schema_name} /${metadata_dir}

echo Step 4: Create Hive table

#beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "CREATE EXTERNAL TABLE ${db_name}.${table_name} ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' location 'hdfs://nameservice1/${top_dir}/${table_name}' TBLPROPERTIES (   'avro.schema.url'='hdfs://nameservice1/${metadata_dir}/${schema_name}') " 

beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -f /tmp/${table_name}.ddl

########
## Initial sqooping and partitioning of a table
## This is intended to run once only per table !!!
## If re-running on the same table, the partitioned table needs to be dropped and the Sqoop job deleted.
## TODO: Partitioning will work right now only for timestamp column that will be partitioned by year and month. This needs serious generalization work
#######

#set -e

source common_env.sh

[ $# -eq 0 ] && { echo "Usage: $0 table_name num_mappers"; exit 1; }

echo Step 0: Cleanup
sqoop job --delete ${job_name}
hdfs dfs -rm /${metadata_dir}/${schema_name}
hdfs dfs -rm -r /${top_dir}/${partitioned_table}
hdfs dfs -rm -r /${staging_dir}/${full_table_name}
beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${partitioned_table}" 2>&1 | grep FAIL


echo Step 1: Create Sqoop Job

sqoop job --create  ${job_name} --meta-connect ${sqoop_metastore} -- import --connect ${connect_string} --username ${user_name} --password-file ${password_file} --table ${full_table_name} -m ${mappers} --incremental append --check-column ${check_column} --as-avrodatafile --warehouse-dir /${staging_dir}

echo Step 2: Execute Sqoop Job
sqoop job -exec ${job_name} --meta-connect ${sqoop_metastore}

echo Step 3: Extract AVRO schema and load to HDFS
hadoop jar /opt/cloudera/parcels/CDH/lib/avro/avro-tools.jar getschema hdfs://nameservice1/${staging_dir}/${full_table_name}/part-m-00000.avro > /tmp/${schema_name}

hdfs dfs -put /tmp/${schema_name} /${metadata_dir}

echo Step 4: Create staging table and empty partitioned table
beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${staging_table} hdfs://nameservice1/${staging_dir}/${full_table_name} hdfs://nameservice1/${metadata_dir}/${schema_name})" 2>&1 | grep FAIL

beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${partitioned_table} hdfs://nameservice1/${top_dir}/${partitioned_table}/${curr_date} hdfs://nameservice1/${metadata_dir}/${schema_name} --partitions "(year int, month int)")" 2>&1 | grep FAIL

echo Step 5: Move the data to the partitioned table
beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" --hiveconf hive.exec.dynamic.partition.mode=nonstrict --hiveconf hive.exec.dynamic.partition=true -e "from  ${db_name}.${staging_table} insert overwrite table ${db_name}.${partitioned_table} partition(year,month) select *,year(from_unixtime(cast(round(${partition_column}/1000) as bigint))),month(from_unixtime(cast(round(${partition_column}/1000) as bigint)));" 2>&1 | grep FAIL

echo Step 6: Cleanup - drop staging table and move Sqooped data into Archive

beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${staging_table}" 2>&1 | grep FAIL

hdfs dfs -mkdir /${archive_dir}/${full_table_name}
hdfs dfs -mv /${staging_dir}/${full_table_name} /${archive_dir}/${full_table_name}/${curr_date}

echo Step 7: Refresh Impala Metadata
impala-shell -k -i ${IMPALAD} -q "invalidate metadata ${db_name}.${partitioned_table}"

echo Step 8: Validate
impala-shell -k -i ${IMPALAD} -q "select count(*) from ${db_name}.${partitioned_table}"
echo All Done!

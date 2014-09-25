source common_env.sh

pk=${2-${table_name}_id}

[ $# -eq 0 ] && { echo "Usage: $0 table_name [primary key]"; exit 1; }

echo Step 0: Cleanup

beeline  -u "jdbc:hive2://${HIVESERVER}:1000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${staging_table}" 2>&1 | grep FAIL

beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${merged_table}" 2>&1 | grep FAIL

echo Step 1: Execute Sqoop Job
sqoop job -exec ${job_name} --meta-connect ${sqoop_metastore}

echo Step 2: Create Staging Table
if beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${staging_table} hdfs://nameservice1/${staging_dir}/${full_table_name} hdfs://nameservice1/${metadata_dir}/${schema_name})" 2>&1 | grep FAIL
then
	echo Failed to create staging table. Cannot continue with merge.
	exit 1
fi

echo Step 3: Create Merged Table 

if beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${merged_table} hdfs://nameservice1/$top_dir/${partitioned_table}/${curr_date} hdfs://nameservice1/${metadata_dir}/${schema_name} --partitions "(year int, month int)")" 2>&1 | grep FAIL
then
	echo Failed to create merged table. Cannot continue with merge.
	exit 1
fi

echo Step 4: Merge data from Partitioned table and Staging table into Merged table
if beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" --hiveconf hive.exec.dynamic.partition.mode=nonstrict --hiveconf hive.exec.dynamic.partition=true -e "
insert overwrite table ${db_name}.${merged_table} partition  (year,month)
		select * from (
			select  /*+ MAPJOIN(new) */ existing.* from ${partitioned_table} existing
			left outer join 
			(select * from ${staging_table}) new
			on existing.${pk} = new.${pk}
			where new.${pk} is null
			union all
			select new.*, year(from_unixtime(cast(round(${partition_column}/1000) as bigint))) as year,month(from_unixtime(cast(round(${partition_column}/1000) as bigint))) as month from ${staging_table} new ) T" 2>&1 | grep FAIL
then
	echo Merge of new and existing data failed. Double check the primary keys and whether the schema was modified
	exit 1
fi 

echo Step 5: Alter location of partitioned table to point to the new one
# Dropping and re-creating table as a work around. Sentry requires too much privileges for alter table location
#beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "alter table ${partitioned_table} set location 'hdfs://nameservice1/$top_dir/${partitioned_table}/${curr_date}'"

beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${partitioned_table}" 2>&1 | grep FAIL

beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${partitioned_table} hdfs://nameservice1/${top_dir}/${partitioned_table}/${curr_date} hdfs://nameservice1/${metadata_dir}/${schema_name} --partitions "(year int, month int)")" 2>&1 | grep FAIL

beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "msck repair table ${partitioned_table}" 2>&1 | grep FAIL

echo Step 6: Move staging data to archive

hdfs dfs -mv /${staging_dir}/${full_table_name} /${archive_dir}/${full_table_name}/${curr_date}

echo Step 7: Refresh Impala Metadata
impala-shell -k -i pzxdap8priv -q "invalidate metadata ${db_name}.${partitioned_table}"

echo Step 8: Validate
impala-shell -k -i pzxdap8priv -q "select count(*) from ${db_name}.${partitioned_table}"

source common_env.sh

[ $# -eq 0 ] && { echo "Usage: $0 table_name"; exit 1; }

echo Step 0: Cleanup

beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "drop table ${db_name}.${staging_table}" 2>&1 | grep FAIL

echo Step 1: Execute Sqoop Job
sqoop job -exec ${job_name} 2>&1

echo Step 2: Add hive partition
curr_year=`date +"%Y"`
tmp=`date +"%m"`
curr_month=${tmp#0}

if beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "alter table ${partitioned_table} add if not exists partition (year=${curr_year},month=${curr_month})" 2>&1 | grep FAIL
then
	echo Failed to create new partition. Cannot continue with data append.
	exit 1
fi


echo Step 3: Create staging table
if beeline  -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" -e "$(./gen_create_tbl.py ${staging_table} hdfs://nameservice1/${staging_dir}/${full_table_name} hdfs://nameservice1/${metadata_dir}/${schema_name})" 2>&1 | grep FAIL
then
        echo Failed to create staging table. Cannot continue with merge.
        exit 1
fi

echo Step 4: Append data

if beeline -u "jdbc:hive2://${HIVESERVER}:10000/${db_name};principal=${KRB_PRINC}" --hiveconf hive.exec.dynamic.partition.mode=nonstrict --hiveconf hive.exec.dynamic.partition=true -e "
	insert into table ${partitioned_table} partition (year,month) 
	select *,year(from_unixtime(cast(round(${partition_column}/1000) as bigint))) as year,month(from_unixtime(cast(round(${partition_column}/1000) as bigint))) as month 
	from ${db_name}.${staging_table}" 2>&1 | grep FAIL
then
	echo Failed to insert new data into partitioned table.
	exit 1
fi

echo Step 5: Cleanup - drop staging table and move Sqooped data into Archive

beeline -u "jdbc:hive2://${HIVESERVER}:10000/webrep;principal=${KRB_PRINC}" -e "drop table ${db_name}.${staging_table}"  2>&1 | grep FAIL

hdfs dfs -mv /${staging_dir}/${full_table_name} /${archive_dir}/${full_table_name}/`date +"%Y_%m_%d_%H_%M"`

echo Step 6: Refresh Impala Metadata
impala-shell -k -i pzxdap8priv -q "invalidate metadata ${db_name}.${partitioned_table}"

echo Step 7: Validate
impala-shell -k -i pzxdap8priv -q "select count(*) from ${db_name}.${partitioned_table}"

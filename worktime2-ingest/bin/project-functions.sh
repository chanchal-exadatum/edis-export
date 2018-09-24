
#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     :
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################
#                                  Functions                                  #
###############################################################################




#/**
#* Get the batch_id
#*/

function fn_get_batch_id(){

    export BATCH_ID=$(date '+%Y%m%d%H%M%S')

}

function fn_check_batch_id_exist(){

    export BATCH_ID=`cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"`

    if [ "${BATCH_ID}" == "" ]
    then
        exit_code=${EXIT_CODE_FAIL}
    else
        exit_code=${EXIT_CODE_SUCCESS}
    fi

    success_message="Successfully fetched batch_id '${BATCH_ID}'"

    failure_message="Batch_id does not exist"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Import data from mysql to target directory using sqoop
#*@param queue
#*@param database name of the mysql database
#*@param username mysql server username
#*@param password mysql server password
#*@param tablename name of the table
#*@param target_dir name of the target directory
#*@param mappers no of mappers to run for running the sqoop job
#*@param field_terminator character which terminates the field
#*@param mysql_host hostname of mysql server
#*@param mysql_port port of mysql server
#*/

function fn_run_mysql_to_raw_one_time(){

    queue=${1}
    fn_assert_variable_is_set "queue" "${queue}"

    database=${2}
    fn_assert_variable_is_set "database" "${database}"

    username=${3}
    fn_assert_variable_is_set "username" "${username}"

    password=${4}
    fn_assert_variable_is_set "password" "${password}"

    tablename=${5}
    fn_assert_variable_is_set "tablename" "${tablename}"

    target_dir=${6}
    fn_assert_variable_is_set "target_dir" "${target_dir}"

    mappers=${7}
    fn_assert_variable_is_set "mappers" "${mappers}"

    field_terminator=${8}
    fn_assert_variable_is_set "field_terminator" "${field_terminator}"

    mysql_host=${9}
    fn_assert_variable_is_set "mysql_host" "${mysql_host}"

    mysql_port=${10}
    fn_assert_variable_is_set "mysql_port" "${mysql_port}"

    fn_delete_hdfs_directory "${target_dir}"

    sqoop import \
        --connect jdbc:mysql://"${mysql_host}":"${mysql_port}"/"${database}" \
        --username "${username}" \
        --password "${password}" \
        --table "${tablename}" \
        --target-dir "${target_dir}" \
        --m "${mappers}" \
        --fields-terminated-by "${field_terminator}" \
        --null-string '\\N' \
        --null-non-string '\\N'

    exit_code=$?

    if [[ "${exit_code}" != "${EXIT_CODE_SUCCESS}" ]];then

       fn_delete_hdfs_directory "${target_path}"
       fn_exit_with_failure_message "1" "Sqoop import failed for table ${tablename}"

    fi
}

#/**
#* Run sqoop eval to get the max of incremental column and store it in a variable
#*@param db_host hostname of mysql server
#*@param db_port port of mysql server
#*@param database_name name of the database
#*@param username mysql server username
#*@param password mysql server password
#*@param table_name name of the table
#*@param incremental_column column of table on which incremental load needs to be performed
#*/

function fn_get_incremental_date(){

 db_host=${1}

 fn_assert_variable_is_set "db_host" "${db_host}"

 db_port=${2}

 fn_assert_variable_is_set "db_port" "${db_port}"

 database_name=${3}

 fn_assert_variable_is_set "database_name" "${database_name}"

 username=${4}

 fn_assert_variable_is_set "username" "${username}"

 password=${5}

 fn_assert_variable_is_set "password" "${password}"

 table_name=${6}

 fn_assert_variable_is_set "table_name" "${table_name}"

 incremental_column=${7}

 fn_assert_variable_is_set "incremental_column" "${incremental_column}"

   export RAW_INCREMENTAL_DATE=$(sqoop eval \
      --connect jdbc:mysql://"${db_host}":"${db_port}"/"${database_name}" \
      --username "${username}"  \
      --password "${password}" \
      --query "SELECT max(${incremental_column}) FROM ${database_name}.${table_name}")

   export INCREMENTAL_DATE=$(echo $RAW_INCREMENTAL_DATE | cut -d "|" -f 4)

   fn_is_valid_date $INCREMENTAL_DATE

   if [ exit_code == ${EXIT_CODE_FAIL} ];

   then
     fn_exit_with_failure_message "1" "Invalid date INCREMENTAL_DATE = $INCREMENTAL_DATE"

   fi

   exit_code=$?

   success_message="Successfully fetched max ${incremental_column}  date from ${table_name}"

   failure_message="Failed to fetch max ${incremental_column} date from  ${table_name}"

   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Import data from mysql to target directory using sqoop incrementally using an incremental column
#*@param queue job scheduling queue for mapreduce
#*@param database name of the mysql database
#*@param username mysql server username
#*@param password mysql server password
#*@param tablename name of the table
#*@param target_dir name of the target directory
#*@param mappers no of mappers to run for running the sqoop job
#*@param field_terminator character which terminates the field
#*@param mysql_host hostname of mysql server
#*@param mysql_port port of mysql server
#*@param incremental_column column of table on which incremental load needs to be performed
#*/


function fn_run_mysql_to_raw_incremental(){

    queue=${1}
    fn_assert_variable_is_set "queue" "${queue}"

    database=${2}
    fn_assert_variable_is_set "database" "${database}"

    username=${3}
    fn_assert_variable_is_set "username" "${username}"

    password=${4}
    fn_assert_variable_is_set "password" "${password}"

    tablename=${5}
    fn_assert_variable_is_set "tablename" "${tablename}"

    target_dir=${6}
    fn_assert_variable_is_set "target_dir" "${target_dir}"

    mappers=${7}
    fn_assert_variable_is_set "mappers" "${mappers}"

    field_terminator=${8}
    fn_assert_variable_is_set "field_terminator" "${field_terminator}"

    mysql_host=${9}
    fn_assert_variable_is_set "mysql_host" "${mysql_host}"

    mysql_port=${10}
    fn_assert_variable_is_set "mysql_port" "${mysql_port}"

    incremental_column=${11}
    fn_assert_variable_is_set "incremental_column" "${incremental_column}"

    fn_delete_hdfs_directory "${target_dir}"

    fn_get_incremental_date \
      "${mysql_host}" \
      "${mysql_port}" \
      "${database}" \
      "${username}" \
      "${password}" \
      "${tablename}" \
      "${incremental_column}"

    test -f "${LAST_EXTRACTED_DATE_DIR}"/"${tablename}"_date.txt

    exit_code=$?

    if [ $exit_code == ${EXIT_CODE_SUCCESS} ];
    then
      export MIN_DATE=$( cat "${LAST_EXTRACTED_DATE_DIR}"/"${tablename}"_date.txt )
      export SOURCE_CONDITION="${incremental_column} > ${MIN_DATE} and ${incremental_column} <= ${INCREMENTAL_DATE}"

    else
      export SOURCE_CONDITION="${incremental_column} <= ${INCREMENTAL_DATE}"

    fi

    sqoop import \
        --connect jdbc:mysql://"${mysql_host}":"${mysql_port}"/"${database}" \
        --username "${username}" \
        --password "${password}" \
        --table "${tablename}" \
        --target-dir "${target_dir}" \
        --m "${mappers}" \
        --fields-terminated-by "${field_terminator}" \
        --where "${SOURCE_CONDITION}" \
        --null-string '\\N' \
        --null-non-string '\\N'

    exit_code=$?

    if [[ "${exit_code}" != "${EXIT_CODE_SUCCESS}" ]];then

       fn_delete_hdfs_directory "${target_path}"
       fn_exit_with_failure_message "1" "Sqoop import failed for table ${tablename}"
    fi

}



###############################################################################
#                                     End                                     #
###############################################################################


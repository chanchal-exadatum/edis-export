#!/bin/bash

##
#/**
#* This has functions that are to be used for hadoop specific operation such as create , delete,
#*    move files in hdfs, running pig scripts or hive scripts , various kafka operations etc.
#*/
##

###############################################################################
#                               Documentation                               #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : This script consists of all the hadoop related utility functions.      #
#                                                                             #
# Note                                                                        #
#     : 1) If function argument ends with * then its required argument.       #
#       2) If function argument ends with ? then its optional argument.       #
#                                                                             #
###############################################################################
#                             Function Definitions                            #
###############################################################################

###
# Delete HDFS directory
#
# Arguments:
#   directory*
#     - path of the directory to be created
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#

#/**
#* Delete HDFS directory
#*@param directory path to the hdfs directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_delete_hdfs_directory(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  fail_on_error=$2

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" == "$EXIT_CODE_SUCCESS" ]
  then

    hdfs dfs -rm -r "${directory}"

    exit_code=$?

    success_message="Deleted hdfs directory ${directory}"

    failure_message="Failed to delete hdfs directory ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  else

    fn_log_info "HDFS directory ${directory} does not exist"

  fi

}

###
# Create HDFS directory
#
# Arguments:
#   directory*
#     - path of the directory to be created
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#

#/**
#* Create HDFS directory
#*@param directory name of the directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_create_hdfs_directory(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  fail_on_error=$2

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" != "$EXIT_CODE_SUCCESS" ]
  then

    hdfs dfs -mkdir -p "${directory}"

    exit_code=$?

    success_message="Created HDFS directory ${directory}"

    failure_message="Failed to create HDFS directory ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  else

    fn_log_info "HDFS directory ${directory} already exists"

  fi
}

#/**
#* Change permission of hdfs directory
#*@param directory path to the hdfs directory
#*@param dir_chmod permissions to be given to the hdfs directory
#*@param recursive To change permissions of all contents inside hdfs directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_chmod_hdfs_directory(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  dir_chmod=$2

  recursive=$3

  fail_on_error=$4

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" != "$EXIT_CODE_SUCCESS" ]
  then

   fn_log_warn "HDFS directory ${directory} does not exists"

  else

    if [ "${recursive}" != "$BOOLEAN_TRUE" ]
    then

      hdfs dfs -chmod ${dir_chmod} "${directory}"

    else

      hdfs dfs -chmod -R ${dir_chmod} "${directory}"

    fi

    exit_code=$?

    success_message="Updated chmod for HDFS directory ${directory} to ${dir_chmod}"

    failure_message="Failed to update chmod for HDFS directory ${directory} to ${dir_chmod}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  fi
}

#/**
#* Change ownership of hdfs directory
#*@param directory path to the hdfs directory
#*@param user_group name of the user group
#*@param recursive flag to change ownership of files recursively or not
#*@param fail_on_error flag to handle exit code
#*/
function fn_chown_hdfs_directory(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  user_group=$2

  recursive=$3

  fail_on_error=$4

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" != "$EXIT_CODE_SUCCESS" ]
  then

    fn_log_warn "HDFS directory ${directory} does not exists"

  else

    if [ "${recursive}" != "$BOOLEAN_TRUE" ]
    then

      hdfs dfs -chown ${user_group} "${directory}"

    else

      hdfs dfs -chown -R ${user_group} "${directory}"

    fi

    exit_code=$?

    success_message="Updated chown for HDFS directory ${directory} to ${user_group}"

    failure_message="Failed to update chown for HDFS directory ${directory} to ${user_group}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  fi
}

#/**
#* Get the merge of hdfs files
#*@param directory_load path to the directory load
#*@param directory_store path to the directory store
#*@param fail_on_error flag to handle exit code
#*/
function fn_getmerge_hdfs_files(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory_load=$1

  directory_store=$2

  fail_on_error=$3

  fn_assert_variable_is_set "directory_load" "${directory_load}"

  fn_assert_variable_is_set "directory_store" "${directory_store}"

  hdfs dfs -test -e "${directory_load}"

  exit_code=$?

  if [ "${exit_code}" == "$EXIT_CODE_SUCCESS" ]
  then

    hdfs dfs -getmerge "${directory_load}" "${directory_store}"

    exit_code=$?

    success_message="Fetched hdfs files ${directory}"

    failure_message="Failed to fetch hdfs files from ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  else

    fn_log_info "HDFS directory ${directory} does not exist"

  fi

}

#/**
#* Check the last modified time of hdfs
#*@param directory path to directory on hdfs
#*@param fail_on_error flag to handle exit code
#*/
function fn_hdfs_get_last_modified(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  fail_on_error=$2

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" == "$EXIT_CODE_SUCCESS" ]
  then

    hdfs dfs -stat "${directory}"

    exit_code=$?

    if [ "${exit_code}" == "$EXIT_CODE_SUCCESS" ]
    then

        export FILE_CREATE_TMST=$(hdfs dfs -stat "${directory}")

    else

        fn_log_info "HDFS directory ${directory} does not exist"

    fi

    success_message="Fetched hdfs Last Modified Time ${directory}"

    failure_message="Failed to fetch hdfs Last Modified Time ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  fi

}

#/**
#* Get the number of records and give appropriate error logs if exists
#*@param directory path to the directory on hdfs
#*@param fail_on_error flag to handle exit code
#*/
function fn_get_record_count(){

  fn_assert_executable_exists "hdfs" "${BOOLEAN_TRUE}"

  directory=$1

  fail_on_error=$2

  fn_assert_variable_is_set "directory" "${directory}"

  hdfs dfs -test -e "${directory}"

  exit_code=$?

  if [ "${exit_code}" == "$EXIT_CODE_SUCCESS" ]
  then

    export ERROR_RECORD_COUNT=$(hdfs dfs -cat "${directory}" |wc -l)

    success_message="Fetched Record Count ${directory}"

    failure_message="Failed to Read Count  ${directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  else

    fn_log_info "HDFS directory ${directory} does not exist"

  fi

}

###
# Create Kafka Topic
#
# Arguments:
#
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#

#/**
#* Execuate Kafka Topics commands
#*@param args Command line arguments for executing kafka-topics script
#*/
function fn_execute_kafka_topics_command(){

  fn_assert_variable_is_set "KAFKA_HOME" "${KAFKA_HOME}"

  fn_log_info "Executing kafka-topics.sh command with arguments $@"

  ${KAFKA_HOME}/bin/kafka-topics.sh "$@"

  exit_code=$?

  fail_on_error=${BOOLEAN_TRUE}

  success_message="Successfully executed kafka-topics.sh command"

  failure_message="Failed to execute kafka-topics.sh command"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Create external hive database
#*@param db_name name of the hive database
#*@param db_location path to the hive database
#*@param fail_on_error flag to handle exit code
#*/
function fn_create_external_hive_database(){

  fn_assert_executable_exists "hive" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "CONFIG_HOME" "${BOOLEAN_TRUE}"

  db_name=$1

  db_location=$2

  fail_on_error=$3

  fn_assert_variable_is_set "db_name" "${db_name}"

  fn_assert_variable_is_set "db_location" "${db_location}"

  hive -i "${CONFIG_HOME}/hive-env.properties" -e "CREATE DATABASE IF NOT EXISTS ${db_name} LOCATION '${db_location}'"

  exit_code=$?

  success_message="Successfully created ${db_name} database"

  failure_message="Failed to create ${db_name} database"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Run hive commands
#*@param module_home path to the module home
#*@param hive_initialization_script path to the hive initialization script
#*@param hive_script path to the hive script to be executed
#*@param fail_on_error flag to handle exit code
#*/
function fn_run_hive(){

  fn_assert_executable_exists "hive" "${BOOLEAN_TRUE}"

  module_home="${1}"

  fn_assert_variable_is_set "Module Home" "${1}"

  hive_initialization_script="$2"

  fn_assert_file_exists "${hive_initialization_script}" "${BOOLEAN_TRUE}"

  hive_script="$3"

  fn_assert_file_exists "${hive_script}" "${BOOLEAN_TRUE}"

  fail_on_error=$4

  fn_assert_variable_is_set "hive_script" "${hive_script}"

  HIVE_ADDITIONAL_PARAMS="  --hiveconf mapred.job.queue.name=${JOB_QUEUE_NAME} --hivevar LOAD_TMST=${LOAD_TMST} --hivevar BATCH_ID=${BATCH_ID}"

  hive ${HIVE_ADDITIONAL_PARAMS}  -i "${CONFIG_HOME}/hive-env.properties" -i "${hive_initialization_script}" -f "${hive_script}"

  exit_code=$?

  success_message="Successfully executed hive script ${hive_script}"

  failure_message="Failed to execute hive script ${hive_script}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Update the metadata of hive partition
#*@param hive_database name of the hive database
#*@param hive_table name of the hive table
#*@param fail_on_error flag to handle exit code
#*/
function fn_update_hive_partition_metadata(){

  fn_assert_executable_exists "hive" "${BOOLEAN_TRUE}"

  hive_database="$1"

  hive_table="$2"

  fn_assert_variable_is_set "hive_table" "${hive_table}"

  fn_assert_variable_is_set "hive_database" "${hive_database}"

  fail_on_error="$3"

  hive -i "${CONFIG_HOME}/hive-env.properties" -e "USE ${hive_database} ; MSCK REPAIR TABLE ${hive_table}"

  exit_code=$?

  success_message="Successfully updated hive table  ${hive_database}.${hive_table} partition metadata"

  failure_message="Failed to update hive table  ${hive_database}.${hive_table} partition metadata"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


###
# Create Kafka Topic
#
# Arguments:
#
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#

#/**
#* Create Kafka Topic
#*@param topic_name name of the topic
#*@param no_of_partitions
#*@param replication_factor
#*@param fail_on_error flag to handle exit code
#*/
function fn_create_kafka_topic(){

  fn_assert_variable_is_set "KAFKA_HOME" "${KAFKA_HOME}"

  fn_assert_variable_is_set "KAFKA_ZOOKEEPER_CONNECTION_STRING" "${KAFKA_ZOOKEEPER_CONNECTION_STRING}"

  topic_name=$1

  no_of_partitions=$2

  replication_factor=$3

  fn_assert_variable_is_set "topic_name" "${topic_name}"

  fn_assert_variable_is_set "no_of_partitions" "${no_of_partitions}"

  fn_assert_variable_is_set "replication_factor" "${replication_factor}"

  fail_on_error=$4

  ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${KAFKA_ZOOKEEPER_CONNECTION_STRING} --list | grep -Fx  "${topic_name}"

  topic_exists=$?

  if [ "${topic_exists}" == "$EXIT_CODE_SUCCESS" ]
  then

    fn_log_warn "Topic ${topic_name} already exists"

  else

    fn_log_info "Creating kafka topic with arguments $@"

    ${KAFKA_HOME}/bin/kafka-topics.sh \
      --zookeeper ${KAFKA_ZOOKEEPER_CONNECTION_STRING} \
      --create \
      --topic ${topic_name} \
      --partitions ${no_of_partitions} \
      --replication-factor ${replication_factor}

    exit_code=$?

    success_message="Successfully created  kafka topic ${topic_name}"

    failure_message="Failed to create kafka topic ${topic_name}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

  fi

}

#/**
#* Run kafka mirror
#*@param consumer_config_file path to the consumer config file
#*@param producer_config_file path to the producer config file
#*@param no_of_streams number of streams in kafka
#*@param topic_whitelist regex for topics to be included
#*/
function fn_run_kafka_mirror(){

  fn_assert_variable_is_set "KAFKA_HOME" "${KAFKA_HOME}"

  fn_assert_variable_is_set "Consumer Config File" "$1"

  fn_assert_variable_is_set "Producer Config File" "$2"

  fn_assert_variable_is_set "Number of Streams" "$3"

  fn_assert_variable_is_set "Topic Whitelist" "$4"

  ${KAFKA_HOME}/bin/kafka-run-class.sh kafka.tools.MirrorMaker --new.consumer --consumer.config "$1" --producer.config "$2" --num.streams $3 --whitelist "$4"

  exit_code=$?

  exit "${exit_code}"

}

#/**
#* Run camus script
#*@param module_home path to the module home
#*@param camus_properties path to the camus properties file
#*@param output_table_name name of the output table
#*/
function fn_run_camus(){

  fn_assert_executable_exists "hadoop" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "CAMUS_HOME" "${CAMUS_HOME}"

  fn_assert_variable_is_set "MODULE_HOME" "$1"

  fn_assert_variable_is_set "Camus Properties" "$2"

  fn_assert_variable_is_set "Output table name" "$3"

  ${CAMUS_HOME}/bin/camus.sh \
    -Dconf.dir=${MODULE_HOME}/etc/camus \
    -P "$2"

  exit_code=$?

  if [ ${exit_code} -eq $EXIT_CODE_SUCCESS ]
  then

    fn_update_hive_partition_metadata "$3" "${BOOLEAN_TRUE}"

  else

    exit "${exit_code}"

  fi

}

#/**
#* Run pig scripts
#*@param module_home path of the module home
#*@param pig_properties_file path to the pig properties file
#*@param pig_script_file path to the pig script file
#*/
function fn_run_pig(){

  fn_assert_executable_exists "pig" "${BOOLEAN_TRUE}"

  module_home="${1}"

  pig_properties_file="${2}"

  pig_script_file="${3}"

  fn_assert_variable_is_set "module_home" "${module_home}"

  fn_assert_variable_is_set "pig_properties_file" "${pig_properties_file}"

  fn_assert_variable_is_set "pig_script_file" "${pig_script_file}"

  PIG_ADDITIONAL_PARAMS="-param MODULE_HOME=${module_home} ${PIG_ADDITIONAL_PARAMS}"

  PIG_CLASSPATH=${SHARED_LIB}/joda-time-2.1.jar pig \
  -D mapred.job.queue.name=${JOB_QUEUE_NAME} \
  -useHCatalog ${PIG_ADDITIONAL_PARAMS} \
  -param_file "${CONFIG_HOME}/pig-env.properties" \
  -param_file "${pig_properties_file}" "${pig_script_file}"

  exit_code=$?

  success_message="Successfully executed pig script ${pig_script_file}"

  failure_message="Failed to execute pig script ${pig_script_file}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}"


}

#/**
#* Download hadoop file to local
#*@param hadoop_file path to the hadoop file
#*@param target_file path to copy the file on local filesystem
#*@param fail_on_error flag to handle exit code
#*/
function fn_hadoop_download_file(){

  hadoop_file="$1"

  target_file="$2"

  fail_on_error="$3"

  fn_assert_executable_exists "hadoop" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "hadoop_file" "${hadoop_file}"

  fn_assert_variable_is_set "target_file" "${target_file}"

  hadoop fs -copyToLocal "${hadoop_file}" "${target_file}"

  exit_code=$?

  success_message="Successfully downloaded hadoop file ${hadoop_file}"

  failure_message="Failed to download hadoop file ${hadoop_file} to local file ${target_file}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


#/**
#* Copy file from local to hadoop
#*@param local_file path of the local file
#*@param hadoop_directory path of the hadoop directory to copy to
#*@param fail_on_error flag to handle exit code
#*/
function fn_copy_file_from_local_to_hadoop(){

    local_file="$1"

    hadoop_directory="$2"

    fail_on_error="$3"

    fn_assert_variable_is_set "local_file" "${local_file}"

    fn_assert_variable_is_set "hadoop_directory" "${hadoop_directory}"

    hdfs dfs -copyFromLocal "${local_file}" "${hadoop_directory}"

    exit_code=$?

    success_message="Successfully copied local file ${local_file} to hadoop directory ${hadoop_directory}"

    failure_message="Failed to copy local file ${local_file} to hadoop directory ${hadoop_directory}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Move hadoop file from source to destination
#*@param hadoop_file source path of the hadoop file
#*@param target_file destination path of the hadoop file
#*@param fail_on_error flag to handle exit code
#*/
function fn_hadoop_move_file(){

  hadoop_file="$1"

  target_file="$2"

  fail_on_error="$3"

  fn_assert_executable_exists "hadoop" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "hadoop_file" "${hadoop_file}"

  fn_assert_variable_is_set "target_file" "${target_file}"

  hadoop fs -mv "${hadoop_file}" "${target_file}"

  exit_code=$?

  if [ $exit_code -ne $EXIT_CODE_SUCCESS ]

  then

      file_name=$(echo $hadoop_file | rev | cut -d'/' -f1 | rev )

      fn_log_info "${file_name} not present in incoming directory checking in target directory ${target_file}"

      hadoop fs -test -e ${target_file}/$file_name

      exit_code=$?

  fi

   success_message="Successfully moved hadoop file ${hadoop_file} to ${target_file}"

   failure_message="${file_name} not present in both incoming ${hadoop_file} and Target ${target_file}"

   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Put file onto hdfs
#*@param local_file path to the local file
#*@param target_file path to the target file
#*@param fail_on_error flag to handle exit code
#*/
function fn_hadoop_put_file(){

  local_file="$1"

  target_file="$2"

  fail_on_error="$3"

  fn_assert_variable_is_set "local_file" "${local_file}"

  fn_assert_variable_is_set "target_file" "${target_file}"

  hadoop fs -put "${local_file}" "${target_file}"

  exit_code=$?

  if [ $exit_code -ne $EXIT_CODE_SUCCESS ]

  then

      file_name=$(echo $local_file | rev | cut -d'/' -f1 | rev )

      fn_log_info "${file_name} not present in incoming directory checking in target directory ${target_file}"

      hadoop fs -test -e ${target_file}/$file_name

      exit_code=$?

  fi


   success_message="Successfully moved hadoop file ${hadoop_file} to ${target_file}"

   failure_message="${file_name} not present in both incoming ${hadoop_file} and Target ${target_file}"

   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

   rm -rf "${local_file}"

}


#/**
#* Get module home
#*@param bash_source
#*/
function fn_get_module_home()
{

  bash_source="$1"

  fn_assert_variable_is_set "bash_source" "${bash_source}"

  export MODULE_HOME="$(dirname "$(dirname "$(readlink -f ${bash_source})")")"

}

#/**
#* Get the md5 checksum of file
#*@param location path to the file parent
#*@param filename name of the file
#*/
function fn_get_file_checksum() {

  location=${1}

  fn_assert_variable_is_set "location" "${location}"

  filename=${2}

  fn_assert_variable_is_set "filename" "${filename}"

  export checksum=$(md5sum ${location}/${filename} | awk '{print$1}')

  exit_code=$?

  success_message="Successfully executed command to get checksum of ${filename} "

  failure_message="Failed to executed command to get checksum of ${filename} "

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Get the size of file on hdfs
#* If size of file is blank
#*         exit_code = 1
#*@param location path to the hdfs file parent
#*@param filename name of the file
#*/
function fn_get_file_size_hdfs() {

  location=${1}

  fn_assert_variable_is_set "location" "${location}"

  filename=${2}

  fn_assert_variable_is_set "filename" "${filename}"

  export filesize=$( hadoop fs -du -s ${location}/${filename} | awk '{print$1}')

  if [ "${filesize}" == "" ]
  then
        exit_code=1
  fi

  success_message="Successfully executed command to get size of ${filename} "

  failure_message="Failed to executed command to get size of ${filename} "

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Fetching tables from teradata to hadoop in raw layer
#*@param tdch_jar
#*@param job_queue_name
#*@param num_map
#*@param teradata_env
#*@param teradata_database
#*@param teradata_username
#*@param teradata_psswd
#*@param teradata_source_tablename
#*@param source_condition
#*/
fn_teradata_to_raw(){

  tdch_jar=$1

  fn_assert_variable_is_set "tdch_jar" "${tdch_jar}"

  job_queue_name=$2

  fn_assert_variable_is_set "job_queue_name" "${job_queue_name}"

  num_map=$3

  fn_assert_variable_is_set "num_map" "${num_map}"

  teradata_env=$4

  fn_assert_variable_is_set "teradata_env" "${teradata_env}"

  teradata_database=$5

  fn_assert_variable_is_set "teradata_database" "${teradata_database}"

  teradata_username=$6

  fn_assert_variable_is_set "teradata_username" "${teradata_username}"

  teradata_psswd=$7

  fn_assert_variable_is_set "teradata_psswd" "${teradata_psswd}"

  target_path=$8

  fn_assert_variable_is_set "target_path" "${target_path}"

  teradata_source_tablename=$9

  fn_assert_variable_is_set "teradata_source_tablename" "${teradata_source_tablename}"

  source_condition=${10}

  fn_assert_variable_is_set "source_condition" "${source_condition}"

  hadoop jar "${tdch_jar}" \
        com.teradata.connector.common.tool.ConnectorImportTool \
        -D mapreduce.job.queuename=${job_queue_name} \
        -D tdch.num.mappers=${num_map} \
        -classname com.teradata.jdbc.TeraDriver \
       -url jdbc:teradata://${teradata_env}/DATABASE=${teradata_database}\
       -username ${teradata_username} \
       -password ${teradata_psswd} \
       -jobtype hdfs \
       -fileformat textfile \
       -separator '|' \
       -targetpaths "${target_path}"\
       -sourcetable "${teradata_source_tablename}" \
       -sourceconditions "${source_condition}"

  exit_code=$?

  if [[ "${exit_code}" != "${EXIT_CODE_SUCCESS}" ]];then

        fn_delete_hdfs_directory "${target_path}"
        fn_exit_with_failure_message "1" "unable to fetch data from teradata"

  fi

}


###
# Function for DistCp
#
# Arguments:
#   *
#     -
#   fail_on_error?
#     -
#       with error code or just write error message and return.
#

#/**
#* Distributed copying of data from source to target
#*@param source_host_name host name of source
#*@param source_path_for_parsed_data
#*@param target_host_name host name of target
#*@param target_path_for_parsed_data
#*@param queue job scheduling queue for mapreduce
#*@param port port of target
#*/
function fn_distcp(){

   source_host_name=$1;

   source_path_for_parsed_data=$2;

   target_host_name=$3;

   target_path_for_parsed_data=$4;

   queue=$5

   port=$6

   fn_assert_variable_is_set "source_host_name" "${source_host_name}"

   fn_assert_variable_is_set "source_path_for_parsed_data" "${source_path_for_parsed_data}"

   fn_assert_variable_is_set "target_host_name" "${target_host_name}"

   fn_assert_variable_is_set "target_path_for_parsed_data" "${target_path_for_parsed_data}"

   fn_assert_variable_is_set "queue" "${queue}"

   fn_assert_variable_is_set "port" "${port}"

   hadoop distcp -D ipc.client.fallback-to-simple-auth-allowed=true -Dmapreduce.job.queuename=${queue} -overwrite ${source_path_for_parsed_data} ${target_host_name}:{port}/${target_path_for_parsed_data}

   exit_code=$?

   success_message="Successfully transferred files "

   failure_message="Failed to transfer files "

   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

   hadoop fs -rm ${source_path_for_parsed_data}/*

   exit_code=$?

   success_message="Successfully deleted files from transfer directory "

   failure_message="Failed to delete files from transfer directory "

   fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

##
# Function to get Keytab
#
# Arguments:
#   keytab_file_path*
#     - path of the keytab file to be used
#   user*
#     - User for which the keytab file is
#
#

#/**
#* Function to perform Kerberos validation for user
#*@param keytab_file_path keytab file path for validation
#*@param user name of the user
#*/
function fn_generate_keytab(){
    fn_assert_executable_exists "kinit" "${BOOLEAN_TRUE}"

    KEYTAB_FILE_PATH=$1  APPLICATION_USER=$2

    fn_assert_variable_is_set "APPLICATION_USER" "${APPLICATION_USER}"

    fn_assert_variable_is_set "KEYTAB_FILE_PATH" "${KEYTAB_FILE_PATH}"

    exit_code=$?

    if [ "${exit_code}" != "$EXIT_CODE_SUCCESS" ]
    then

    fn_log_warn "Keytab file for user ${APPLICATION_USER} is not present"

    else  kinit -kt ${KEYTAB_FILE_PATH} ${APPLICATION_USER}

    exit_code=$?

    success_message="Kerberose validation successful for user ${APPLICATION_USER}"

    failure_message="Failed to Authenticate Kerberose for user ${APPLICATION_USER}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

    fi

    }

#/**
#* Function to create directory with current date
#*@param directory_path path where the date directory needs to be created
#*/
function fn_create_date_dir(){
    directory_path="$1"

    fn_assert_variable_is_set "directory_path" "${directory_path}"

	dir_date=`date +%Y/%m/%d`

	DIR="${directory_path}/${dir_date}"

	if [ ! -d "${DIR}" ];

	then

    hadoop fs -mkdir -p "${DIR}"

	fi
}

#/**
#* Send mail on a weekly basis and
#*@param tablename name of the table
#*@param team_mail_address mail address of the team / individual
#*/
fn_send_weekly_mail(){

  tablename=${1}

  team_mail_address=${2}

  zero_count=0

  HIVE_ADDITIONAL_PARAMS="  --hiveconf mapred.job.queue.name=${JOB_QUEUE_NAME} --hivevar LOAD_TMST=${LOAD_TMST} --hivevar BATCH_ID=${BATCH_ID}"

  ERR_TBL_COUNT=$(hive ${HIVE_ADDITIONAL_PARAMS} -e "use ${DB_STAGE}; select COUNT(*) from ${tablename} where record_processed_code='N'")

  exit_code=$?

  if [[ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]];then

  if [[ "${ERR_TBL_COUNT}" != "${zero_count}" ]]; then

    echo "Hi Team, Table_Name : ${tablename} , Error_Record_Count : ${ERR_TBL_COUNT}" | mail -s "${tablename} has ${ERR_TBL_COUNT} error records that are not processed" "${team_mail_address}"

  fi

    else

       fn_exit_with_failure_message "1" "unable to fetch count from ${tablename}"

  fi

}

#/**
#* Get the max value of column in hive table and handle errors appropriately
#*@param db_name name of the database
#*@param tbl_name name of the hive table
#*@param col_name name of the column inside the table
#*/
fn_get_max_col_val_from_tbl(){

  db_name=${1}

  tbl_name=${2}

  col_name=${3}

  fn_assert_variable_is_set "db_name" "${db_name}"

  fn_assert_variable_is_set "tbl_name" "${tbl_name}"

  fn_assert_variable_is_set "col_name" "${col_name}"

  HIVE_ADDITIONAL_PARAMS="  --hiveconf mapred.job.queue.name=${JOB_QUEUE_NAME} --hivevar LOAD_TMST=${LOAD_TMST} --hivevar BATCH_ID=${BATCH_ID}"

  export MAX_COL_VAL=$(hive ${HIVE_ADDITIONAL_PARAMS} -e "use ${db_name}; select MAX(${col_name}) from ${tbl_name}")

  exit_code=$?

  if [[ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]];then

    if [[ "${MAX_COL_VAL}" == "" ]]; then

      fn_exit_with_failure_message "1" "unable to fetch MAX ${col_name} from ${tbl_name}"

    fi

   else

     fn_exit_with_failure_message "1" "unable to fetch MAX ${col_name} from ${tbl_name}"

  fi

}

#/**
#* Get the number of records in hive table
#*@param db_name name of the database
#*@param tbl_name name of the hive table
#*/
fn_get_recrd_cnt_from_tbl(){

  db_name=${1}

  tbl_name=${2}

  fn_assert_variable_is_set "db_name" "${db_name}"

  fn_assert_variable_is_set "tbl_name" "${tbl_name}"

  HIVE_ADDITIONAL_PARAMS="  --hiveconf mapred.job.queue.name=${JOB_QUEUE_NAME} --hivevar LOAD_TMST=${LOAD_TMST} --hivevar BATCH_ID=${BATCH_ID}"

  export RECORD_COUNT=$(hive ${HIVE_ADDITIONAL_PARAMS} -e "use ${db_name}; select COUNT(*) from ${tbl_name}")

  exit_code=$?

  if [[ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]];then

    if [[ "${RECORD_COUNT}" == "" ]]; then

      fn_exit_with_failure_message "1" "unable to fetch COUNT from ${tbl_name}"

    fi

   else

     fn_exit_with_failure_message "1" "unable to fetch COUNT from ${tbl_name}"

  fi

}

#/**
#* Set dates in condition
#*@param source_condition
#*/
function fn_set_dates_in_condition() {

    SOURCE_CONDITION=${1}

    fn_assert_variable_is_set "SOURCE_CONDITION" "${SOURCE_CONDITION}"

    export SOURCE_CONDITION=$(sed "s/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/'&'/g" <<< "${SOURCE_CONDITION}")

    exit_code=$?

    if [[ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]];then

        if [[ "${SOURCE_CONDITION}" == "" ]]; then

          fn_exit_with_failure_message "1" "SET PROPER CONDITION"

        fi

     else

     fn_exit_with_failure_message "1" "SET PROPER CONDITION"

    fi

}

#/**
#* Set timestamp in condition
#*@param source_condition
#*/
function fn_set_timestamp_in_condition() {

        SOURCE_CONDITION=${1}

        fn_assert_variable_is_set "SOURCE_CONDITION" "${SOURCE_CONDITION}"

        export SOURCE_CONDITION=$(sed "s/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/'& 00:00:00.0'/g" <<< "${SOURCE_CONDITION}")

        echo "$SOURCE_CONDITION"

         exit_code=$?

    if [[ "${exit_code}" == "${EXIT_CODE_SUCCESS}" ]];then

        if [[ "${SOURCE_CONDITION}" == "" ]]; then

          fn_exit_with_failure_message "1" "SET PROPER CONDITION"

        fi

     else

     fn_exit_with_failure_message "1" "SET PROPER CONDITION"

    fi

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

################################################################################
#                                     End                                      #
################################################################################

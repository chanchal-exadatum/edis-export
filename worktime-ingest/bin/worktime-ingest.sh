
#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     :                                                                       #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################
#                           Identify Script Home                              #
###############################################################################

#Find the script file home
pushd . > /dev/null
SCRIPT_DIRECTORY="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_DIRECTORY}" ]);
do
  cd "`dirname "${SCRIPT_DIRECTORY}"`"
  SCRIPT_DIRECTORY="$(readlink "`basename "${SCRIPT_DIRECTORY}"`")";
done
cd "`dirname "${SCRIPT_DIRECTORY}"`" > /dev/null
SCRIPT_DIRECTORY="`pwd`";
popd  > /dev/null
MODULE_HOME="`dirname "${SCRIPT_DIRECTORY}"`"

###############################################################################
#                           Import Dependencies                               #
###############################################################################

if [ "${CONFIG_HOME}" == "" ]
then

     PROJECT_HOME="`dirname "${MODULE_HOME}"`"
     CONFIG_HOME="${PROJECT_HOME}/config"

fi

. ${CONFIG_HOME}/bash-env.properties
. ${MODULE_HOME}/bin/constants.sh
. ${MODULE_HOME}/bin/log-functions.sh
. ${MODULE_HOME}/bin/common-functions.sh
. ${MODULE_HOME}/bin/common-file-functions.sh
. ${MODULE_HOME}/bin/hadoop-functions.sh
. ${MODULE_HOME}/bin/teradata-tpt-functions.sh
. ${MODULE_HOME}/bin/oracle-spool-functions.sh
. ${MODULE_HOME}/etc/worktime-ingest.shell.properties



###############################################################################
#                                Main                                         #
###############################################################################


database_user_name=$1
database_user_password=$2
database_sid=$3
config_file=$4

BATCH_ID=$(date '+%Y%m%d%H%M%S')

OUTPUT_DIRECTORY=${ORACLE_OUTPUT_FILE_DIRECTORY}/${BATCH_ID}

fn_create_local_directory ${OUTPUT_DIRECTORY}

if [ -f "${min_record_file}" ]
then
    fn_log "last stored offset found at ${min_record_file}"

	  fn_get_value_from_file "${min_record_file}"

	  min_value=${val}

	  spool_input_file=${MODULE_HOME}/etc/tmp/worktime-export.sql


else

    fn_run_oracle_spool ${database_user_name} ${database_user_password} ${database_sid} ${MODULE_HOME}/etc/spool/worktime-min-record.sql

	  fn_get_value_from_file "${min_record_file}"

	  min_value=${val}

    spool_input_file=${MODULE_HOME}/etc/tmp/worktime-export-first.sql

fi


fn_run_oracle_spool ${database_user_name} ${database_user_password} ${database_sid} ${MODULE_HOME}/etc/spool/worktime-max-record.sql

fn_get_value_from_file ${max_record_file}

max_value=${val}

fn_assert_variable_is_set "min_value" "${min_value}"

fn_assert_variable_is_set "max_value" "${max_value}"

spool_file=${MODULE_HOME}/etc/spool/worktime-export-${BATCH_ID}.sql

output_file=${OUTPUT_DIRECTORY}/worktime-${BATCH_ID}.dat

cat ${spool_input_file} >> ${spool_file}

min_value="$(echo -e "${min_value}" | sed -e 's/^[[:space:]]*//')"
max_value="$(echo -e "${max_value}" | sed -e 's/^[[:space:]]*//')"

fn_replace_value_in_file "min_value" "${min_value}" "${spool_file}"

fn_replace_value_in_file "max_value" "${max_value}" "${spool_file}"

fn_replace_value_in_file "output_file" "${output_file}" "${spool_file}"

fn_run_oracle_spool ${database_user_name} ${database_user_password} ${database_sid} ${spool_file}

echo $max_value > ${min_record_file}

exit_code=$?

success_message="Successfully saved ${max_value} in file ${min_record_file}"

failure_message="Failed to save ${max_value} in file ${min_record_file}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

java -jar ${MODULE_HOME}/lib/WorkScheduleProcessor.jar ${output_file} ${config_file} ${PARSER_OUTPUT_PATH}/${BATCH_ID}

exit_code=$?

success_message="Successfully parsed ${output_file} "

failure_message="Failed to parse ${output_file}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

store_labor_employee_file=${MODULE_HOME}/etc/spool/worktime-export-${BATCH_ID}.bteq

cat ${MODULE_HOME}/etc/tmp/STORE_LABOR_EMPLOYEE.bteq >> ${store_labor_employee_file}

store_labor_employee_path=${PARSER_OUTPUT_PATH}/${BATCH_ID}/STORE_LABOR_EMPLOYEE/STORE_LABOR_EMPLOYEE

fn_replace_value_in_file "filepath" "${store_labor_employee_path}" "${store_labor_employee_file}"

fn_run_taradata_bteq_script < ${store_labor_employee_file}

exit_code=$?

success_message="Successfully bteq script ${store_labor_employee_file} "

failure_message="Failed bteq script ${store_labor_employee_file}"

###############################################################################
#                                     End                                     #
###############################################################################


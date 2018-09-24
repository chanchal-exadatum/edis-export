
#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : This script consists oracle utility functions such as Spool,sqlplus             #
#                                                                             #
# Note                                                                        #
#     : 1) If function argument ends with * then its required argument.       #
#       2) If function argument ends with ? then its optional argument.       #
#                                                                             #
###############################################################################
#                             Function Definitions                            #
###############################################################################

###
# run oracle spool job
#
# Arguments:
#   script_file
#     - sqlplus spool file containing query
#
function fn_run_oracle_spool(){
  database_user_name=$1
  database_user_password=$2
  database_sid=$3
  script_file=$4

  #fn_assert_executable_exists "sqlplus" "${BOOLEAN_TRUE}"
  fn_assert_executable_exists "sqlplus64" "${BOOLEAN_TRUE}"
  fn_assert_variable_is_set "database_user_name" "${database_user_name}"
  fn_assert_variable_is_set "database_user_password" "${database_user_password}"
  fn_assert_variable_is_set "database_sid" "${database_sid}"
  fn_assert_variable_is_set "script_file" "${script_file}"

  #sqlplus ${database_user_name}/${database_user_password}@${database_sid} @${script_file}
  sqlplus64 ${database_user_name}/${database_user_password}@//localhost:1521/${database_sid} @${script_file}
  exit_code=$?

  success_message="Successfully executed Spool script ${script_file}"

  failure_message="Failed to execute Spool script ${script_file}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}"


}



################################################################################
#                                     End                                      #
################################################################################


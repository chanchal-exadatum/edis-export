
#!/bin/bash
###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : This script consists teradata utility functions such as BTEQ,TPT             #
#                                                                             #
# Note                                                                        #
#     : 1) If function argument ends with * then its required argument.       #
#       2) If function argument ends with ? then its optional argument.       #
#                                                                             #
###############################################################################
#                             Function Definitions                            #
###############################################################################

###
# run tpt job
#
# Arguments:
#   script_file
#     - tpt script file
#   script_job_file
#     - contains output file properties
#   teradata_config_job_file
#     - contains teradata server & database credential information
#
function fn_run_taradata_tpt_script(){
  script_file=$1
  script_job_file=$2
  teradata_config_job_file=$3


  fn_assert_executable_exists "tbuild" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "script_file" "$script_file"

  fn_assert_variable_is_set "script_job_file" "$script_job_file"

  fn_assert_variable_is_set "teradata_config_job_file" "$teradata_config_job_file"

  tbuild -f $script_file -v $script_job_file -v $teradata_config_job_file -C

  exit_code=$?

  success_message="Successfully executed TPT script $1"

  failure_message="Failed to execute TPT script $1"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}"


}

###
# run Teradata Bteq job
#
# Arguments:
#   bteq_script_file
#     - bteq script file
#
function fn_run_taradata_bteq_script(){

  bteq_script_file=$1

  fn_assert_executable_exists "bteq" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "bteq_script_file" "$bteq_script_file"

  bteq<$bteq_script_file

  exit_code=$?

  success_message="Successfully executed Bteq script ${bteq_script_file}"

  failure_message="Failed to execute TPT script ${bteq_script_file}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}"


}




################################################################################
#                                     End                                      #
################################################################################


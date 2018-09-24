#!/bin/bash

##
#/**
#* This has functions that can be used for local operation such as moving file ,changing permissions etc.
#*/
##

###############################################################################
#                               Documentation                                 #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : This script consists of all the common utility functions.             #
#                                                                             #
# Note                                                                        #
#     : 1) If function argument ends with * then its required argument.       #
#       2) If function argument ends with ? then its optional argument.       #
#                                                                             #
###############################################################################
#                             Function Definitions                            #
###############################################################################


###
# Assert if given variable is set.
# If variable is,
#    Set - Do nothing
#    Empty - Exit with failure message
#
# Arguments:
#   variable_name*
#     - Name of the variable
#   variable_value*
#     - Value of the variable
#

#/**
#* Assert if given variable is set.
#* If variable is,
#*    Set - Do nothing
#*    Empty - Exit with failure message
#*@param variable_name name of the variable
#*@param variable_value value of the variable
#*/
function fn_assert_variable_is_set(){

  variable_name=$1

  variable_value=$2

  if [ "${variable_value}" == "" ]
  then

    exit_code=${EXIT_CODE_VARIABLE_NOT_SET}

    failure_messages="${variable_name} variable is not set"

    fn_exit_with_failure_message "${exit_code}" "${failure_messages}"

  fi

}

###
# Log given failure message and exit the script
# with given exit code
#
# Arguments:
#   exit_code*
#     - Exit code to be used to exit with
#   failure_message*
#     - Failure message to be logged
#


#/**
#* Log given failure message and exit the script with given exit code
#*@param exit_code exit code to be used to exit with
#*@param failure_message failure message to be logged
#*/
function fn_exit_with_failure_message(){

  exit_code=$1

  failure_message=$2

  fn_log_error "${failure_message}"

  exit ${exit_code}

}


###
# Check the exit code.
# If exit code is,
#   Success - Log the success message and return
#   Failure - Log the failure message and exit with
#             the same exit code based on flag.
#
# Arguments:
#   exit_code*
#     - Exit code to be checked
#   success_message*
#     - Message to be logged if the exit code is zero
#   failure_message*
#     - Message to be logged if the exit code is non-zero
#   fail_on_error?
#     - If this flag is set then exit with the same exit code
#       Else write error message and return.
#

#/**
#* Check the exit code.
#* If exit code is,
#*    Success - Log the success message and return
#*    Failure - Log the failure message and exit with the same exit code based on flag
#*@param exit_code exit code to be checked
#*@param success_message message to be logged if the exit code is zero
#*@param failure_message message to be logged if the exit code is non zero
#*@param fail_on_error flag to handle the exit code
#*/
function fn_handle_exit_code(){

  exit_code=$1

  success_message=$2

  failure_message=$3

  fail_on_error=$4

  if [ "${exit_code}" != "$EXIT_CODE_SUCCESS" ]
  then

    fn_log_error "${failure_message}"

    if [ "${fail_on_error}" != "$EXIT_CODE_SUCCESS" ]
    then

      exit ${exit_code}

    fi

  else

    fn_log_info "${success_message}"

  fi

}


###
# Check if the executable/command exists or not
#
# Arguments:
#   exit_code*
#     - Exit code to be checked
#   fail_on_error?
#     - If this flag is set then exit with the same exit code
#       Else write error message and return.
#

#/**
#* Check if the executable/command exists or not
#*@param executable path to the executable file
#*@param fail_on_error flag to handle the exit code
#*/

function fn_assert_executable_exists() {

  executable=$1

  fail_on_error=$2

  if ! type "${executable}" > /dev/null; then

    fn_log_error "Executable ${executable} does not exists"

    if [ "${fail_on_error}" == "${BOOLEAN_TRUE}" ];
    then

        exit ${EXIT_CODE_EXECUTABLE_NOT_PRESENT}

    fi
  fi

}

#/**
#* Check if file exists or not.
#*@param some_file path to the file
#*@param fail_on_error flag to handle the exit code
#*/
function fn_assert_file_exists() {

  some_file="$1"

  fail_on_error=$2

  if [ ! -f "${some_file}" ]; then

    fn_log_error "File ${some_file} does not exists"

    if [ "${fail_on_error}" == "${BOOLEAN_TRUE}" ];
    then

        exit ${EXIT_CODE_EXECUTABLE_NOT_PRESENT}

    fi
  fi

}

#/**
#* Check if file is empty or not.
#*@param file_to_be_checked path to the file
#*/
function fn_assert_file_not_empty(){

  file_to_be_checked="${1}"

  fn_assert_variable_is_set "file_to_be_checked" "${file_to_be_checked}"

  fn_assert_file_exists "${file_to_be_checked}" "${BOOLEAN_TRUE}"

  if [ ! -s "${file_to_be_checked}" ]; then

    fn_log_error "File ${file_to_be_checked} is empty"

    exit ${EXIT_CODE_FILE_IS_EMPTY}

  fi

}

#/**
#* Run jar
#*@param module_home path to the module home
#*@param args command line arguments to be passed along with the java command
#*/
function fn_run_java(){

  fn_assert_executable_exists "java" "${BOOLEAN_TRUE}"

  module_home="${1}"

  fn_assert_variable_is_set "MODULE_HOME" "${module_home}"

  MODULE_CLASSPATH=""

  for i in $module_home/lib/*.jar; do
    MODULE_CLASSPATH=$MODULE_CLASSPATH:$i
  done

  MODULE_CLASSPATH=`echo $MODULE_CLASSPATH | cut -c2-`

  java -cp "${MODULE_CLASSPATH}" ${@:2}

  exit_code=$?

  success_message="Successfully executed java command for module ${module_home}"

  failure_message="Failed to execute java command for module ${module_home} ${@:2}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


#/**
#* Change ownership of local directory
#* If recursively is,
#*    True - set the -R flag for changing permission of all files in the directory
#*@param directory_path path to the directory
#*@param user_name name of the user
#*@param recursively flag to recursively change permissions of all files in the directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_chown_local_directory(){

  directory_path="$1"

  user_name="$2"

  recursively="$3"

  fail_on_error="$4"

  fn_assert_variable_is_set "directory_path" "${directory_path}"

  fn_assert_variable_is_set "user_name" "${user_name}"

  RECURSIVELY=""
  if [ "${recursively}" == "${BOOLEAN_TRUE}" ]; then

    RECURSIVELY="-R"

  fi

  chown ${RECURSIVELY} "${user_name}" "${directory_path}"

  exit_code=$?

  success_message="Successfully changed ownership of directory ${directory_path} to ${user_name}"

  failure_message="Failed to change ownership of directory ${directory_path} to ${user_name}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Create a local directory
#*@param directory_path path to the directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_create_local_directory(){

  directory_path="$1"

  fail_on_error="$2"

  fn_assert_variable_is_set "directory_path" "${directory_path}"

  mkdir -p "${directory_path}"

  exit_code=$?

  success_message="Successfully created directory ${directory_path}"

  failure_message="Failed to create directory ${directory_path}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Delete the directory and all its contents
#*@param directory_path path to the directory
#*@param fail_on_error flag to handle exit code
#*/
function fn_delete_recursive_local_directory(){

  directory_path="$1"

  fail_on_error="$2"

  fn_assert_variable_is_set "directory_path" "${directory_path}"

  rm -r "${directory_path}"

  exit_code=$?

  success_message="Successfully deleted directory ${directory_path}"

  failure_message="Failed to delete directory ${directory_path}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


#/**
#* Delete a file
#*@param file_name path to the file
#*@param fail_on_error flag to handle exit code
#*/
function fn_delete_local_file(){

  file_name="$1"

  fail_on_error="$2"

  fn_assert_variable_is_set "file_name" "${file_name}"

  rm  "${file_name}"

  exit_code=$?

  success_message="Successfully deleted file ${file_name}"

  failure_message="Failed to delete file ${file_name}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Check if file does not exist
#*@param file_name path to the file
#*/
function fn_assert_file_does_not_exists(){

  file_name="$1"

  fn_assert_variable_is_set "file_name" "${file_name}"

  test -f "${file_name}"

  exit_code=$?

  if [ "${exit_code}" != "${EXIT_CODE_FAIL}" ];

  then

      failure_message="${file_name} file exists"

      fn_exit_with_failure_message "1" "${failure_message}"

  fi

}

#/**
#* Move file from source to destination and provide logging for errors occured
#*@param source_file path to the source file
#*@param target_file path to move the target file
#*/
function fn_mv_local_file(){

    source_file="$1"

    target_file="$2"

    fn_assert_variable_is_set "source_file" "${source_file}"

    fn_assert_variable_is_set "target_file" "${target_file}"

    mv "${source_file}" "${target_file}"

    exit_code=$?

    success_message="Successfully renamed file ${source_file} to ${target_file}"

    failure_message="Failed to rename file ${source_file} to ${target_file}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Functionality to send mail
#*@param message contents of the mail body
#*@param subject subject of the mail
#*@param target_mail mail address of the reciever
#*@param attachment path to attachments to be added in the mail
#*/
function fn_send_mail(){

    message="$1"

    subject="$2"

    target_mail="$3"

    attachment="$4"

    fn_assert_variable_is_set "message" "${message}"

    fn_assert_variable_is_set "target_mail" "${target_mail}"

    fn_assert_variable_is_set "subject" "${subject}"

    test -e "mail -s "${subject}" -a ${attachment} ${target_mail}"

    exit_code=$?

    echo "${message}" | mail -s "${subject}" -a ${attachment} ${target_mail}


    success_message="Successfully mailed  to ${target_mail}"

    failure_message="Failed to mail to ${target_mail}"

    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#/**
#* Check if date is valid
#*@param date date string
#*/
function fn_is_valid_date() {
  # yyyy-mm-dd

  date="$1"

  fn_assert_variable_is_set "date" "${date}"

  if [[ "${date}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && date -d "${date}">/dev/null 2>&1;

  then
     $exit_code = ${EXIT_CODE_SUCCESS}

  else
     $exit_code = ${EXIT_CODE_FAIL}

  fi;
}

################################################################################
#                                     End                                      #
################################################################################

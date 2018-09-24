#!/bin/bash

# #%L
# server
# %%
# Copyright (C) 2016 - 2017 Exadatum Software Services Priveate Limited
# %%
# All information contained herein is, and remains the property of
# Exadatum Software Services Private Limited. The intellectual and
# technical concepts contained herein are proprietary to Exadatum
# Software Services Private Limited and may be covered by U.S, India
# and Foreign Patents, patents in process, and are protected by trade
# secret or copyright law. Dissemination of this information or
# reproduction of this material is strictly forbidden unless prior
# written permission is obtained from COMPANY.  Access to the source
# code contained herein is hereby forbidden to anyone except current
# COMPANY employees, managers or contractors who have executed
# Confidentiality and Non-disclosure agreements explicitly covering
# such access.
# #L%

#                               General Details                               #
#                                                                             #
# Description                                                                 #
#     : This script consists of all the common file functions.             #
#                                                                             #
# Note                                                                        #
#     : 1) If function argument ends with * then its required argument.       #
#       2) If function argument ends with ? then its optional argument.       #
#                                                                             #
#                             Function Definitions                            #

# Move source directory to target location
#
# Arguments:
#   source_directory*
#     - path of source directory
#   target directory*
#     - path of target direcctory
#

function fn_move_local_directory(){
  source_directory_path="$1"

  target_directory_path="$2"

  fail_on_error="$3"

  fn_assert_variable_is_set "source_directory_path" "${source_directory_path}"

  fn_assert_variable_is_set "target_directory_path" "${target_directory_path}"

  mv "${source_directory_path}" "${target_directory_path}"

  exit_code=`fn_get_exit_code $?`

  success_message="Successfully created directory ${directory_path}"

  failure_message="Failed to create directory ${directory_path}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


# Returns value from file
#
# Arguments:
#   file path*
#     - path of input file
#
function fn_get_value_from_file() {

filename="$1"

fn_assert_variable_is_set "filename" "${filename}"

value_from_file=$(cat "${filename}")

exit_code=$?

success_message="Successfully fetched record  ${value_from_file} from file ${filename}"

failure_message="Failed to fetch last stored offset ${filename}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

export val=${value_from_file}

}

# Replace source string with target value in file
#
# Arguments:
#   source_value*
#     - the source string
#   target_value*
#     - the target string
#   input_file*
#     - the input file contains source value
#
function fn_replace_value_in_file() {

source_value=$1

target_value=$2

input_file=$3

fn_assert_variable_is_set "source value" "${source_value}"

fn_assert_variable_is_set "target value" "${target_value}"

fn_assert_variable_is_set "input file" "${input_file}"

sed -i "s|${source_value}|${target_value}|g" ${input_file}

exit_code=$?

success_message="Successfully replaced ${source_value} with ${target_value} in file ${input_file} "

failure_message="Failed to replaced ${source_value} with ${target_value} in file ${input_file} "

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



#                                     End                                      #

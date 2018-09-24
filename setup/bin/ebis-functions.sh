
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
#                                  Functions                                  #
###############################################################################


function fn_get_batch_id(){
    export BATCH_ID=$(date '+%Y%m%d%H%M%S')
}

function fn_get_persisted_batch_id(){

    test -f "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

     exit_code=$?
     if [ $exit_code == ${EXIT_CODE_FAIL} ]

     then

        fn_handle_exit_code "${exit_code}" "BatchId Exists" "BatchID not exists" "${fail_on_error}"

     else

      export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

     fi

     export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

}

function fn_run_gobblin(){

  pull_file=$1

  fail_on_error=$2

  fn_assert_variable_is_set "pull_file" "${pull_file}"

  fn_assert_variable_is_set "GOBBLIN_HOME" "${GOBBLIN_HOME}"

  bash ${GOBBLIN_HOME}/bin/gobblin-mapreduce.sh --conf  ${pull_file}

  exit_code=$?

  success_message="Successfully ran gobblin job "

  failure_message="Failed to run gobblin job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_run_custom_camus(){

  camus_properties=$1

  fail_on_error=$2

  fn_assert_variable_is_set "camus_properties" "${camus_properties}"

  fn_assert_variable_is_set "CAMUS_HOME" "${CAMUS_HOME}"

  bash ${CAMUS_HOME}/bin/camus-run -P  ${camus_properties}

  exit_code=$?

  success_message="Successfully ran camus job "

  failure_message="Failed to run camus job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



function fn_run_spark(){

  spark_class=$1

  jar=$2

  configuration_file=$3

  batch_id=$4

  fail_on_error=$5

  fn_assert_executable_exists "spark-submit" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "jar" "${jar}"

  spark-submit \
       --class ${spark_class} \
       --master yarn-client \
       --conf spark.hadoop.validateOutputSpecs=false \
       ${jar} ${configuration_file} ${batch_id}

  exit_code=$?

  success_message="Successfully ran spark transform job "

  failure_message="Failed to run spark transform job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_run_spark_transform_on_yarn(){

  spark_class=$1

  jar=$2

  external_jar=$3

  external_file=$4

  input_path=$5

  output_path=$6

  xsd=$7

  fail_on_error=$8

  fn_assert_executable_exists "spark-submit" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "jar" "${jar}"

  fn_assert_variable_is_set "external_jar" "${external_jar}"

  fn_assert_variable_is_set "external_file" "${external_file}"

  fn_assert_variable_is_set "input_path" "${input_path}"

  fn_assert_variable_is_set "output_path" "${output_path}"

  fn_assert_variable_is_set "xsd" "${xsd}"

  spark-submit \
       --class ${spark_class} \
       --master yarn-client \
       --conf spark.hadoop.validateOutputSpecs=false \
       --jars ${external_jar} \
       --files ${external_file} \
       ${jar} ${input_path} ${output_path} ${xsd} ${external_file}

  exit_code=$?

  success_message="Successfully ran spark transform job "

  failure_message="Failed to run spark transform job"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_run_java(){

  classpath=$1

  classname=$2

  variable=${@:3}

  fn_assert_executable_exists "java" "${BOOLEAN_TRUE}"

  fn_assert_variable_is_set "classpath" "${classpath}"

  fn_assert_variable_is_set "classname" "${classname}"

  java -cp ${classpath} ${classname} ${variable}

  exit_code=$?

  success_message="Successfully ran ${classname} "

  failure_message="Failed to run ${classname}"

  fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



###############################################################################
#                                     End                                     #
###############################################################################


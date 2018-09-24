
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
. ${MODULE_HOME}/bin/hadoop-functions.sh
. ${MODULE_HOME}/bin/ebis-functions.sh
. ${MODULE_HOME}/etc/config.sh

###############################################################################
#                                Main                                         #
###############################################################################


test -f "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

exit_code=$?

if [ $exit_code == ${EXIT_CODE_FAIL} ]

then

  fn_create_local_directory "${BATCH_ID_DATA_DIR}"
  fn_get_batch_id
  echo "${BATCH_ID}" > "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}"

else

  export BATCH_ID=$(cat "${BATCH_ID_DATA_DIR}"/"${BATCH_ID_FILE_NAME}")

fi

echo "Successfully Created Batch ID"




###############################################################################
#                                     End                                     #
###############################################################################

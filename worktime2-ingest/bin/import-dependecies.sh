#!/bin/bash

##
#/**
#* This is a utility script for importing all the functions in your bash script.
#*/
##

#Load common dependencies

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
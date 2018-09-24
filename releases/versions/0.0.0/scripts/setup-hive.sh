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


fn_create_external_hive_database "${ENV_PREFIX}db_work" "${HADOOP_ROOT_DIR}/db_work"

fn_create_external_hive_database "${ENV_PREFIX}db_gold" "${HADOOP_ROOT_DIR}/db_gold"

fn_create_external_hive_database "${ENV_PREFIX}db_stage" "${HADOOP_ROOT_DIR}/db_stage"

fn_create_external_hive_database "${ENV_PREFIX}db_trans" "${HADOOP_ROOT_DIR}/db_trans"



################################################################################
#                                     End                                      #
################################################################################

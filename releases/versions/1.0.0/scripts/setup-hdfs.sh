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
#                           Setup Hive Directories                            #
###############################################################################


fn_create_hdfs_directory "${DB_RAW_XML_DIR}"

fn_create_hdfs_directory "${DB_RAW_CSV_DIR}"

fn_create_hdfs_directory "${DB_RAW_JSON_DIR}"

fn_create_hdfs_directory "${DB_STAGE_XML_DIR}"

fn_create_hdfs_directory "${DB_STAGE_CSV_DIR}"

fn_create_hdfs_directory "${DB_STAGE_JSON_DIR}"

fn_create_hdfs_directory "${DB_GOLD_XML_DIR}"

fn_create_hdfs_directory "${DB_GOLD_CSV_DIR}"

fn_create_hdfs_directory "${DB_GOLD_JSON_DIR}"


################################################################################
#                                     End                                      #
################################################################################


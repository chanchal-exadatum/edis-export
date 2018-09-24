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
#                        Setup HDFS Layer Directories                         #
###############################################################################


fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/stage/in"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/raw"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/gold"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/smith"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/stage/out"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/system"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/default/temp"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/stage/in"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/raw"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/gold"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/smith"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/stage/out"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/system"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/archive/temp"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/stage/in"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/raw"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/gold"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/smith"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/stage/out"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/system"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/backup/temp"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/stage/in"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/raw"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/gold"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/smith"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/stage/out"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/system"

fn_create_hdfs_directory "${HADOOP_ROOT_DIR}/security/temp"




################################################################################
#                                     End                                      #
################################################################################


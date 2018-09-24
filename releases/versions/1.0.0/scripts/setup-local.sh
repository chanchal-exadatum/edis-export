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
#                           Setup Local Directories                           #
###############################################################################

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/clean-up"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/setup"

fn_create_local_directory  "${BATCH_ID_DATA_DIR}"

fn_create_local_directory  "${LAST_EXTRACTED_DATE_DIR}"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-csv"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-json"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/ingest-xml"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-csv"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-json"

fn_create_local_directory  "${WORKFLOW_TMP_DIR}/transform-xml"


################################################################################
#                                     End                                      #
################################################################################


#!/bin/bash

##
#/**
#* This has aliases defined for various hadoop commands etc.
#*/
##

###############################################################################
#                               Documentation                               #
###############################################################################
#                                                                             #
# Description                                                                 #
#     : This script consists of all the linux related utility functions.      #
#                                                                             #
#                                                                             #
###############################################################################
#                             Command Definitions                             #
###############################################################################


#List hadoop fs commands
alias hfs="hadoop fs"
#List files
alias hls="hfs -ls"
#List files recursive
alias hlsr="hls -R"
#List files sorted by time
alias hlst="hls -t"
#List files recursive sorted by time
alias hlsr="hlsr -t"
#Make hadoop directory
alias hmkdir="hfs -mkdir"
#Make hadoop directory and its parents
alias hmkdirs="hmkdir -p"
#Moves files from source to destination
alias hmv="hfs -mv"
#Move from local to hadoop file system
alias hmvfl="hfs -moveFromLocal"
#Move from hadoop file system to local
alias hmvtl="hfs -moveToLocal"
#Move from hadoop file system to local
alias hput="hfs -put"
#Remove path
alias hrm="hfs -rm"
#Remove path recursively
alias hrmr="hrm -R"
#Remove directory
alias hrmdir="hfs -rmdir"
#Remove directory
alias htail="hfs -tail -f"
#Copy files
alias hcpf="hfs -cp"
#Copy and overwrite if exists
alias hcpie="hcpf -f"


################################################################################
#                                     End                                      #
################################################################################

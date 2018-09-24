---
-- #%L
-- xarchitect-migrator
-- %%
-- Copyright (C) 2016 - 2018 Exadatum Software Services Private Limited
-- %%
-- All information contained herein is, and remains the property of
-- Exadatum Software Services Private Limited. The intellectual and
-- technical concepts contained herein are proprietary to Exadatum
-- Software Services Private Limited and may be covered by U.S, India
-- and Foreign Patents, patents in process, and are protected by trade
-- secret or copyright law. Dissemination of this information or
-- reproduction of this material is strictly forbidden unless prior
-- written permission is obtained from COMPANY.  Access to the source
-- code contained herein is hereby forbidden to anyone except current
-- COMPANY employees, managers or contractors who have executed
-- Confidentiality and Non-disclosure agreements explicitly covering
-- such access.
-- #L%
---
set colsep '|'
set echo off
set feedback off
set pagesize 0
set sqlprompt ''
set headsep off
set term off
set feed off:wq
set linesize 32767
set long 32767
set longchunksize 32767
set trimout on
set trimspool on

spool '/home/exa00077/Documents/spool_test/worktime/20180920205215/worktime-20180920205215.dat'

select xmlquery from worktime where id>='15' and id<='28';

spool off
EXIT


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
set echo off
set feedback off
set pagesize 0
set sqlprompt ''
set trimspool on
set headsep off
set term off
set feed off:wq

spool '/home/exa00077/Documents/spool_test/worktime-min-record.value'

select min(id) from worktime;

spool off

EXIT

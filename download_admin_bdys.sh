#!/bin/sh

user=lds_bde
host=localhost
database=linz_db

#run python imprt
echo downloading source files
python download_admin_bdys.py

#validation of imports
echo validating imported tables
q1=`psql -d $database -U $user -f aimsref_validation.sql` | grep -oh [0-9]*$
v1=eval $q1 | awk '{ sum+=$1} END {print sum}'

#final migration step
if [ $v1 -eq 0 ];
then
        #final migration step
        echo mapping tables
        psql -h $host -d $database -U $user -f aimsref_commands.sql
else
        echo $v1 errors in validation
fi

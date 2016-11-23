#### CONVERT THIS SCRIPT TO PYTHON!

#!/usr/bin/env bash
source /env/config.sh
mysql -h $host -u $user -p$pass < /etl-scripts/new_etl_process/daily_update.sql

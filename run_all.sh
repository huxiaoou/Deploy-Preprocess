#!/usr/bin/bash -l

echo "$(date +'%Y-%m-%d %H:%M:%S') begins"
udb=$(python -c $'import yaml\nwith open("config.yaml", "r") as f:_config = yaml.safe_load(f)\nprint(_config["dbs"]["user"])')
echo "user_db=$udb"

if [ "$#" -eq 1 ]; then
    if [ "$1" = "--auto" ]; then
        end_date=$(date +"%Y%m%d")
    else
        end_date="$1"
    fi
else
    read -p "Please input the end date, format = [YYYYMMDD]:" end_date
fi
echo "end_date = $end_date"

rm_tqdb $udb --table preprocess
rm_tqdb $udb --table dominant
rm_tqdb $udb --table avlb
rm_tqdb $udb --table mkt

bgn_date="20160104"
bgn_date_icov="20160503"

python main.py preprocess --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') preprocess data created"

python main.py dominant --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') dominant data created"

python main.py avlb --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') avlb data created"

python main.py mkt --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') mkt data created"

python main.py icov --bgn $bgn_date_icov --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') icov data created"

python main.py css --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') css data created"

python main.py srets --bgn $bgn_date --end $end_date
echo "$(date +'%Y-%m-%d %H:%M:%S') srets data created"

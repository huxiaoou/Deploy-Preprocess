bgn_date="20160104"
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

python main.py preprocess --bgn $bgn_date --end $end_date
python main.py dominant --bgn $bgn_date --end $end_date
python main.py avlb --bgn $bgn_date --end $end_date
python main.py mkt --bgn $bgn_date --end $end_date

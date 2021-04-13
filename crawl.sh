
## this shell is used for epa crawler
## curl the epa file by api


##
##
## this is crawler for normal station
##
##


## input argument
fpath=$1
nam=$2
staNum=$3
off=$4
yr_mon=$5
apiKey=$6

## parameter
url="https://data.epa.gov.tw/api/v1/${staNum}?format=csv&limit=1000&offset=${off}&year_month=${yr_mon}&api_key=${apiKey}"
fname=$fpath/$nam

## download data
echo download $nam
curl -s -o $fname $url -H "accept: */*"
fsize=$(stat -c%s "$fname")

## judge the file has data or not
if [ $fsize -lt 100 ]; then
	rm $fname
fi
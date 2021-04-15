
## this shell is used for epa crawler
## curl the epa file by api


## input argument
fpath=$1
nam=$2
staNum=$3
off=$4
yr_mon=$5
apiKey='8decfe1b-6c94-4f04-8fa7-f64c9652daf4'

## parameter
url="https://data.epa.gov.tw/api/v1/${staNum}?format=csv&limit=1000&offset=${off}&year_month=${yr_mon}&api_key=${apiKey}"
fname=$fpath/$nam

## download data
curl -s -o $fname $url -H "accept: */*" -f

## judge the file has data or not
if [ -f "$fname" ]; then

	fsize=$(stat -c%s "$fname")
	
	if [ $fsize -lt 100 ]; then
		rm $fname
	fi
fi 
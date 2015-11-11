rowcount=10000
insertcount=1000

rm data/source.csv
rm finalcount/*
mkdir finalcount
mkdir data

curl http://www.opendatacache.com/data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv | gunzip -c | head -n "$rowcount" >> data/source.csv

node import.js data/source.csv $rowcount $insertcount
wait
finalcount=$(ls finalcount)

if [ $finalcount == $((rowcount-1)) ]; then
  echo "All rows successfully pushed"
  node finalSQL.js
else
  echo "something went wrong, the rowcounts do not match"
fi

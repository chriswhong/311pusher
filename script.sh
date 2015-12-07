env=$1

if [ $1 = "dev" ]; then
  rowcount=1001
  insertcount=100
  tablename=union_311_dev
else
  rowcount=1500001
  insertcount=500
  tablename=union_311
fi

rm data/*
rm finalcount/*
mkdir data
mkdir finalcount

curl http://www.opendatacache.com/data.cityofnewyork.us/api/views/erm2-nwe9/rows.csv | gunzip -c | head -n "$rowcount" >> data/source.csv

node import.js data/source.csv $rowcount $insertcount $tablename

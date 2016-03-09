env=$1

if [ $1 = "dev" ]; then
  rowcount=1000
  insertcount=10
  tablename=union_311_dev
else
  rowcount=1000
  insertcount=10
  tablename=union_311
fi

rm data/*
rm finalcount/*
mkdir data
mkdir finalcount

curl "https://data.cityofnewyork.us/resource/fhrw-4uyv.csv" | head -n "$rowcount" >> data/source.csv

node import.js data/source.csv $rowcount $insertcount $tablename

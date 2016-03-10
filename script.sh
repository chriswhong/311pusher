env=$1

if [ $1 = "dev" ]; then
  rowcount=10
  insertcount=1
  tablename=union_311_dev
else
  rowcount=8000
  insertcount=100
  tablename=union_311
fi

rm data/*
rm finalcount/*
mkdir data
mkdir finalcount

dlcount=$(( rowcount + 1 ))

curl "https://data.cityofnewyork.us/resource/fhrw-4uyv.csv?\$LIMIT=10000000&\$ORDER=created_date%20DESC" | head -n "$dlcount" >> data/source.csv

node import.js data/source.csv $rowcount $insertcount $tablename

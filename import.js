var LineByLineReader = require('line-by-line'),
  Mustache = require('mustache'),
  CSV = require('csv-string'),
  request = require('request'),
  fs = require('fs'),
  nodemailer = require('nodemailer')
  moment = require('moment'),
  dns = require('dns')
require('dotenv').load();


var sourceFile = process.argv[2];
var sourceRowcount = process.argv[3];
var insertCount = process.argv[4];
var tableName = process.argv[5];
var scratchTableName = tableName + '_scratch';
  var header;

var cdbConfig = {
  username: process.env.USERNAME,
  apikey: process.env.APIKEY
}

var CartoDB = require('cartodb');
 
var sql = new CartoDB.SQL({
  user: process.env.USERNAME, 
  api_key: process.env.APIKEY
})


var totalCount = 0,
  batchCount = 0,
  valueStrings = [],
  lr,
  lastBatch = false, 
  errorCount;


truncateTable();





function truncateTable() {
 console.log('Truncating table...');
  sql.execute('TRUNCATE TABLE ' + tableName + '_scratch')
    .on('done', function(res) {
      console.log(res);
      console.log('Success, pushing data to CartoDB...')
      pushData();
    })
}

function pushData() {

  var firstLine = true;


  lr = new LineByLineReader(sourceFile);

  lr.on('line', function (line) {
    
    if(firstLine) {

      firstLine=false;
      header = CSV.parse(line);
    } else {
      line = CSV.parse(line);
      
      var data = {};
      line[0].forEach( function(value, i) {
        data[header[0][i]] = line[0][i]
      })


      var valueString = buildValueString(data);
      valueStrings.push(valueString); 
      batchCount++;
      // totalCount++;
    }


    if(batchCount==insertCount) {
      lr.pause();
      processBatch();
    }
      
  });

  lr.on('error', function (err) {
      // 'err' contains error object
  });

  lr.on('end', function () {
      console.log('Last chunk!')
      //process the last chunk
      lastBatch = true;
      processBatch();
  });
}

function buildValueString(data) {

  //transform individual data elements when data is an object, then CSV.stringify() it.
  for (var key in data) {

    //escape single quotes
    data[key] = data[key].replace(/'/g, "''");

    //convert times to GMT
    if (key == 'closed_date' || key == 'created_date' || key == 'resolution_action_updated_date') {
      data[key] = shiftTime(data[key])
    } else {
      
    }


    if(data[key]>0) {
      data[key] = '\'' + data[key] + '\'';
    } else {
      data[key] = 'null'
    }
    //location:POINT (-73.891815 40.859385)

    console.log(data);

  }

  var values = CSV.stringify(data).replace(/(\r\n|\n|\r|")/gm,"");
  var valueString = '(' + values + ')';

  console.log(valueString);
  
  return valueString;
}

function processBatch() {
  console.log('Pushing ' + batchCount + ' rows...')
  
  if (batchCount>0) {
    var query = buildInsertQuery(valueStrings);
    sql.execute(query)
      .on('done', function(res) {
        console.log(res);
        totalCount += res.total_rows;
        console.log('Total pushed: '+ totalCount)
        batchCount = 0;
        valueStrings.length = 0;
        
        console.log(lastBatch)
        if(lastBatch == true) {
          checkSize();
        } else {
          lr.resume();
        }
      })
      .on('_error', function(res) {
        console.log(res);
      })
  
  } else {
    checkSize();
  }


}


function buildInsertQuery() {
  var allValues = '';
  //concatenate valueStrings
  valueStrings.forEach(function(valueString, i) {
    allValues += valueString;

    if(i<valueStrings.length-1) {
      allValues += ',';
    } 
  });


  var template = 'INSERT into {{scratchTableName}} ({{header}}) VALUES {{{allValues}}}';

  var query = Mustache.render(template,{
    allValues: allValues,
    header: CSV.stringify(header),
    scratchTableName: scratchTableName
  });

  console.log(query);

  return query;

}



function checkSize() {

  sql.execute('SELECT count(*) FROM ' + scratchTableName)
    .on('done', function(res) {
      var count = res.rows[0].count;
      console.log('I count ' + count + ' rows in the scratch table');
      console.log(count,sourceRowcount-1)
      if (count >= sourceRowcount-1) {
        //appendMaster();
        renameTables();
      }
    })
}

function renameTables() {
  var query = Mustache.render('ALTER TABLE {{tableName}} RENAME TO {{tableName}}_old; ALTER TABLE {{scratchTableName}} RENAME TO {{tableName}}; ALTER TABLE {{tableName}}_old RENAME TO {{scratchTableName}}',{
    tableName: tableName,
    scratchTableName: scratchTableName
  });

  sql.execute(query)
    .on('done', function(res) {
      console.log(res);
      checkFinalSize();
    })

}

function checkFinalSize() {
  console.log('Verifying rowcount in production table...');

  sql.execute('SELECT count(*) FROM ' + tableName)
    .on('done', function(res) {
      console.log('Rowcount: ' + res.rows[0].count);
    })
}


//shift time to GMT
function shiftTime(timestamp) {
  timestamp = moment(timestamp).add(5,'hours').format('MM/DD/YYYY hh:mm:ss A');
  return timestamp;
}

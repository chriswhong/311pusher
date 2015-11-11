var LineByLineReader = require('line-by-line'),
    Mustache = require('mustache'),
    CSV = require('csv-string'),
    request = require('request'),
    fs = require('fs');

    require('dotenv').load();

var memwatch = require('memwatch-next');
memwatch.on('leak', function(info) {
 console.error('Memory leak detected: ', info);
});

var sourceFile = process.argv[2];
var sourceRowcount = process.argv[3];
var insertCount = process.argv[4];

var cdbConfig = {
  username: process.env.USERNAME,
  apikey: process.env.APIKEY
}

var totalCount = 0,
  batchCount = 0,
  valueStrings = [];

var lr;

console.log('Truncating table...');
executeSQL('TRUNCATE TABLE etltest',function(res){
  console.log(res);
 if(!res.error) {
  pushData();
 } else {
  console.log(res.error)
 }
});

function pushData() {
  console.log('pushdata!')

  var firstLine = true;
  var header;

  lr = new LineByLineReader(sourceFile);

  lr.on('line', function (line) {
    
    if(firstLine) {
      //setHeader(line);
      firstLine=false;
    } else {
      line = CSV.parse(line);
      var valueString = buildValueString(line[0]);
      valueStrings.push(valueString); 
      batchCount++;
      totalCount++;
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
      //process the last chunk
      processBatch();
      // make check the table size
      executeSQL('SELECT count(*) FROM etltest;',function(res) {
        if(!res.error) {
          var count = res.rows[0].count;
          console.log('I count ' + count + ' rows in the CartoDB table');
          console.log(count,sourceRowcount-1)
          if (count == sourceRowcount-1) {
            setgeom();
          }
        } else {
          console.log(res.error)
        }
      });
  });
}

function setgeom() {
  //setting the_geom
  executeSQL('UPDATE etltest SET the_geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326)',function(res) {
    if(!res.error) {
      appendMaster();
    } else {
      console.log(res.error);
    }
  })
}


function appendMaster() {
  executeSQL('TRUNCATE TABLE union_311; INSERT into union_311 SELECT * FROM etltest',function(res) {
    if(!res.error) {
      console.log(res);
      console.log('Done!')
    } else {
      console.log(res.error);
    }
  })
}


function processBatch() {
  var query = buildInsertQuery(valueStrings);
    executeSQL(query,function() {
      console.log(totalCount + ' rows processed!')
      batchCount = 0;
      valueStrings.length = 0;
      lr.resume();
    });
}

//executes SQL API calls
function executeSQL(sql,cb) {
  console.log('Executing SQL...')

  var options = {
    username: cdbConfig.username,
    apikey: cdbConfig.apikey
  }

  var url = Mustache.render('https://{{username}}.cartodb.com/api/v2/sql?&api_key={{apikey}}',options);

  request.post({
    url:     url,
    form:    { q: sql }
  }, function(error, response, body) {
    cb(JSON.parse(body));
  });
}

function setHeader(line) {
  header = line.split(',');
  
}

function buildValueString(line) {
  var valueString = '(';
  line.forEach(function(value, i) {
    //escape single quotes
    value = value.replace(/'/g, "''");

    if(value.length>0) {
      valueString += '\'' + value + '\'';
    } else {
      valueString += 'null'
    }
    
    if(i<line.length-1) {
      valueString += ',';
    } else {
      valueString += ')'
    }

  });

  return valueString;
}

function buildInsertQuery() {
  var allValues = '';
  valueStrings.forEach(function(valueString,i) {
    allValues += valueString;
    if(i<valueStrings.length-1) {
      allValues += ',';
    } 
  });


  var template = 'INSERT into etltest (unique_key,created_date,closed_date,agency,agency_name,complaint_type,descriptor,location_type,incident_zip,incident_address,street_name,cross_street_1,cross_street_2,intersection_street_1,intersection_street_2,address_type,city,landmark,facility_type,status,due_date,resolution_description,resolution_action_updated_date,community_board,borough,x_coordinate_state_plane,y_coordinate_state_plane,park_facility_name,park_borough,school_name,school_number,school_region,school_code,school_phone_number,school_address,school_city,school_state,school_zip,school_not_found,school_or_citywide_complaint,vehicle_type,taxi_company_borough,taxi_pick_up_location,bridge_highway_name,bridge_highway_direction,road_ramp,bridge_highway_segment,garage_lot_name,ferry_direction,ferry_terminal_name,latitude,longitude,location) VALUES {{{allValues}}}';

  var query = Mustache.render(template,{allValues: allValues});

  return query;

}

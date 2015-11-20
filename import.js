var LineByLineReader = require('line-by-line'),
  Mustache = require('mustache'),
  CSV = require('csv-string'),
  request = require('request'),
  fs = require('fs'),
  nodemailer = require('nodemailer')
  moment = require('moment');

require('dotenv').load();

// create reusable transporter object using SMTP transport 
var transporter = nodemailer.createTransport({
    service: 'Gmail',
    auth: {
        user: process.env.EMAIL,
        pass: process.env.EMAILPASS
    }
});

var sourceFile = process.argv[2];
var sourceRowcount = process.argv[3];
var insertCount = process.argv[4];
var tableName = process.argv[5];
var scratchTableName = tableName + '_scratch';

var cdbConfig = {
  username: process.env.USERNAME,
  apikey: process.env.APIKEY
}

var totalCount = 0,
  batchCount = 0,
  valueStrings = [],
  lr,
  lastBatch = false;

console.log('Truncating table...');
executeSQL('TRUNCATE TABLE ' + tableName + '_scratch', function(res){
  console.log(res);
  if(!res.error) {
    console.log('Success, pushing data to CartoDB...')
    pushData();
  } else {
    console.log(res.error)
  }
});

function pushData() {

  var firstLine = true;
  var header;

  lr = new LineByLineReader(sourceFile);

  lr.on('line', function (line) {
    
    if(firstLine) {
      firstLine=false;
    } else {
      line = CSV.parse(line);
      var valueString = buildValueString(line[0]);
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

// function setgeom() {
//   console.log('Setting the_geom...')
//   executeSQL('UPDATE ' + scratchTableName + ' SET the_geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326)',function(res) {
//     if(!res.error) {
//       console.log(res);
//       appendMaster();
//     } else {
//       console.log(res.error);
//     }
//   })
// }

function appendMaster() {
  console.log('Appending to production table..');
  var sql = Mustache.render('TRUNCATE TABLE {{tableName}}; INSERT into {{tableName}} SELECT * FROM {{scratchTableName}}',{
    tableName: tableName,
    scratchTableName: scratchTableName
  });
  executeSQL(sql, function(res) {
    if(!res.error) {
      console.log(res);
      checkFinalSize();
    } else {
      console.log(res.error);
    }
  })
}

function renameTables() {
  var sql = Mustache.render('ALTER TABLE {{tableName}} RENAME TO {{tableName}}_old; ALTER TABLE {{scratchTableName}} RENAME TO {{tableName}}; ALTER TABLE {{tableName}}_old RENAME TO {{scratchTableName}}',{
    tableName: tableName,
    scratchTableName: scratchTableName
  });
  executeSQL(sql, function(res) {
    if(!res.error) {
      console.log(res);
      checkFinalSize();
    } else {
      console.log(res.error);
    }
  })
}

function checkFinalSize() {
  console.log('Verifying rowcount in production table...');
  executeSQL('SELECT count(*) FROM ' + tableName, function(res) {
    if(!res.error) {
      console.log('Rowcount: ' + res.rows[0].count);
      sendNotification(res.rows[0].count);
    } else {
      console.log(res.error);
    }
  })
}

function sendNotification(count) {
  // setup e-mail data with unicode symbols 
  var message = 'The 311 script completed and wrote ' + count + ' into table ' + tableName + '.  I just thought you might want to know...';

  var mailOptions = {
      from: 'Chris Whong ✔ <chris.m.whong@gmail.com>', // sender address 
      to: 'chris.m.whong@gmail.com', // list of receivers 
      subject: '311 Script Complete', // Subject line 
      text: message
  };
   
  // send mail with defined transport object 
  transporter.sendMail(mailOptions, function(error, info){
      if(error){
          return console.log(error);
      }
      console.log('Message sent: ' + info.response);
  });
}

function checkSize() {
  executeSQL('SELECT count(*) FROM ' + scratchTableName, function(res) {
    if(!res.error) {
      var count = res.rows[0].count;
      console.log('I count ' + count + ' rows in the scratch table');
      console.log(count,sourceRowcount-1)
      if (count >= sourceRowcount-1) {
        //appendMaster();
        renameTables();
      }
    } else {
      console.log(res.error)
    }
  });
}

function processBatch() {
  console.log('Pushing ' + batchCount + ' rows...')
  
  if (batchCount>0) {
    var query = buildInsertQuery(valueStrings);
    executeSQL(query,function(res) {
    if (res.error) {
      console.log("There was an error, trying again",res.error)
      processBatch();
    } else {
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
    }
    
    
  });
  } else {
    checkSize();
  }


}

//executes SQL API calls
function executeSQL(sql,cb) {
  
  var options = {
    username: cdbConfig.username,
    apikey: cdbConfig.apikey
  }

  var url = Mustache.render('https://{{username}}.cartodb.com/api/v2/sql?&api_key={{apikey}}',options);

  request.post({
    url:     url,
    form:    { q: sql }
  }, function(error, response, body) {
    if(!error) {
      try {
        cb(JSON.parse(body));
      } catch (e) {
        console.log(body)
        cb({
          error:true,
          response: body
        })
      }
	     
	   } else {
      console.log(error)
    }	

  });
}

function setHeader(line) {
  header = line.split(',');
  
}

function buildValueString(line) {
  var valueString = '',
  coord = {};

  line.forEach(function(value, i) {
    //convert times to GMT prior to insert
    if(i == 1 || i==2 || i==20) {
      if (value.length>0) {
         value = shiftTime(value);
      }
    }

    if(i==51) {
      coord.lon = value;
    }

    if(i==50) {
      coord.lat = value;    
    }

    //escape single quotes
    value = value.replace(/'/g, "''");

    if(value.length>0) {
      valueString += '\'' + value + '\'';
    } else {
      valueString += 'null'
    }
    
    if(i<line.length-1) {
      valueString += ',';
    } 

  });


  var makePoint;
  if(coord.lat.length>0 && coord.lon.length>0) {
    makePoint = Mustache.render('ST_SetSRID(ST_MakePoint({{lon}}, {{lat}}), 4326)',coord);
  } else {
    makePoint = 'null';
  }
 

  valueString = Mustache.render('({{{makePoint}}},{{{valueString}}})',{
    valueString: valueString,
    makePoint: makePoint
  });

  //ST_SetSRID(ST_MakePoint(long, lat), 4326);
  return valueString;
}

function shiftTime(timestamp) {
  timestamp = moment(timestamp).add(5,'hours').format('MM/DD/YYYY hh:mm:ss A');
  return timestamp;
}

function buildInsertQuery() {
  var allValues = '';
  valueStrings.forEach(function(valueString,i) {
    allValues += valueString;
    if(i<valueStrings.length-1) {
      allValues += ',';
    } 
  });


  var template = 'INSERT into {{scratchTableName}} (the_geom,unique_key,created_date,closed_date,agency,agency_name,complaint_type,descriptor,location_type,incident_zip,incident_address,street_name,cross_street_1,cross_street_2,intersection_street_1,intersection_street_2,address_type,city,landmark,facility_type,status,due_date,resolution_description,resolution_action_updated_date,community_board,borough,x_coordinate_state_plane,y_coordinate_state_plane,park_facility_name,park_borough,school_name,school_number,school_region,school_code,school_phone_number,school_address,school_city,school_state,school_zip,school_not_found,school_or_citywide_complaint,vehicle_type,taxi_company_borough,taxi_pick_up_location,bridge_highway_name,bridge_highway_direction,road_ramp,bridge_highway_segment,garage_lot_name,ferry_direction,ferry_terminal_name,latitude,longitude,location) VALUES {{{allValues}}}';

  var query = Mustache.render(template,{
    allValues: allValues,
    scratchTableName: scratchTableName
  });


  return query;

}

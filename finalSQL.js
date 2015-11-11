var request = require('request'),
  fs = require('fs'),
  Mustache = require('mustache');

require('dotenv').load();

var cdbConfig = {
  username: process.env.USERNAME,
  apikey: process.env.APIKEY
}

//setting the_geom
executeSQL('UPDATE etltest SET the_geom = ST_SetSRID(ST_MakePoint(longitude,latitude),4326)',function(res) {
  if(!res.error) {
    appendMaster();
  } else {
    console.log(res.error);
  }
})

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
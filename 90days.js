//https://data.cityofnewyork.us/resource/fhrw-4uyv.csv?$LIMIT=3000000&$ORDER=created_date DESC&$WHERE=created_date>='2015-12-12'


//seems to work when space and single quotes are URL escaped
//https://data.cityofnewyork.us/resource/fhrw-4uyv.csv?$LIMIT=3000000&$ORDER=created_date%20DESC&$WHERE=created_date>=%272015-12-12%27

var Mustache = require('mustache'),
  CartoDB = require('cartodb'),
  Moment = require('moment');

require('dotenv').load();

var sourceLimit = 1000000;
var ninetyDaysAgo = Moment().subtract(90, 'days').format('YYYY-MM-DD');

var sourceTemplate = 'https://data.cityofnewyork.us/resource/fhrw-4uyv.csv?$LIMIT={{sourceLimit}}&$ORDER=created_date%20DESC&$WHERE=created_date>=%272015-12-12%27';


var sourceURL = Mustache.render( sourceTemplate, { sourceLimit: sourceLimit });

console.log(sourceURL);

var importer = new CartoDB.Import({
  user:process.env.USERNAME, 
  api_key:process.env.APIKEY
});

var sql = new CartoDB.SQL({
  user: process.env.USERNAME, 
  api_key: process.env.APIKEY
})

console.log('Importing NYC 311 data since ' + ninetyDaysAgo + ' into CartoDB...')

importer
  .url( sourceURL , { privacy: 'public' })
  .on('done', function(table_name) {
    console.log('Success! The table ' + table_name + ' has been created!');
    changeNames(table_name)
  })
  .on('_error', function(res) {
    console.log(res)
  });


function changeNames(table_name) {
  console.log('Renaming the new table...')

  sql.execute('DROP TABLE IF EXISTS three_one_one; ALTER TABLE {{table_name}} RENAME TO three_one_one', { table_name: table_name })
    .on('done', function(res) {
      console.log(res);
      console.log('Success, 311plus has been updated.')
    })
    .on('_error', function(res) {
      console.log(res);
    })
}


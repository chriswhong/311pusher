# 311pusher
shell & node scripts that push chunks of 311 data to cartodb using the SQL api

### Overview
This set of scripts takes care of ETL for this [NYC 311 Data Downloader](http://chriswhong.github.io/311plus/).  It currently runs each night at midnight (eastern time), pulling data from opendatacache.com and pushing it to CartoDB 1000 rows at a time.

### Methodology
NYC publishes 311 data via their Socrata-powered open data portal.  The 2010 to present dataset contains over 7 million records as of November 2015.  [Opendatacache](http://www.opendatacache.com/) by [talos](https://github.com/talos) makes downloading this data easier and faster by using for gzip compression.  (It is simply sending a file instead of a stream from a database)


`script.sh` downloads a user-defined number of rows from the NYC 311 2010 to present dataset on opendatacache using curl.  It then runs `import.js`

`import.js` does the following:

- Truncate {scratchtablename} on CartoDB
- Parse the downloaded CSV n lines at a time (n is also defined in `script.sh`)
- Builds a point geometry and shifts the timestamp columns to GMT+0 for each row
- Builds an INSERT statement for n rows, POSTs it to {scratchtablename}
- Checks the size of the scratch table to make sure all the rows are present
- Renames the production table to {productiontablename_old}, renames the scratch table to {productiontablename}, renames {productiontablename_old} to {scratchtablename}
- Sends an email notification

###Environment Variables
Create a `.env` file with the following:
USERNAME={yourcartodbusername}
APIKEY={yourcartodbapikey}
EMAIL={gmailaddress}
EMAILPASS={gmaillesssecureappspassword} (You need to configure gmail to allow "less secure apps", you can't just use it as an SMTP mailer anymore)

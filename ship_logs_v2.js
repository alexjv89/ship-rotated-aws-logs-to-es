/**
 * this is the latest version of the ship logs. This relies on having a good 
 * index of rotated logs in elastic search. You can get this through
 */

// Get rotated logs
// For each rotated log, retrieve lines
// Batch and send the lines to production


var config = require('./config.js');
var async = require('async');
var request = require('request');
var fs = require('fs');
var zlib     = require('zlib');
var readline = require('readline');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({accessKeyId:config.accessKeyId,secretAccessKey:config.secretAccessKey});
var stats={
	lines_count:0,
	file_end_pos:{},
}
var relevantKeys=[];
if(fs.existsSync('./nodejs.log'))
	fs.unlinkSync("./nodejs.log"); // delete the file, to start with and empty file


/**
 * this gets all the log files from elastic search that match a file type
 * and between certain time frame
 * @return {[type]} [description]
 */


/**
 * this is used for getting the short key from long key
 * @param  {[type]} longKey [description]
 * @return {[type]}         [description]
 */
var getShortKey=function(longKey){
	var temp = longKey.split('/');
	shortKey = temp[temp.length-1];
	return shortKey;
}
// console.log(JSON.stringify(body));





/**
 * Any transformations that you want to do to this line
 * @param  {[type]} line [description]
 * @return {[type]}      [description]
 */
var transformLine = function(line){
	line = line.replace('"app_env":"production"','"app_env":"pro"');
	line = line.replace('"app_env":"prod"','"app_env":"pro"');
	line = line.replace('"app_env":"development"','"app_env":"dev"');
	return line;
}

/**
 * downloads one file and saves it to /logs as a .gz
 * @param  {[type]}   bucket   [description]
 * @param  {[type]}   key      [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
var downloadOneFile=function(bucket,key,callback){
	shortKey = getShortKey(key);
	if (fs.existsSync('logs/'+shortKey)) {
    console.log('  - file already downloaded');
		callback(null);
	}
	else{
		console.log('  - starting to download a file now');
		s3.getObject({Bucket:bucket,Key:key},function(err,data){
			if(err){
				// console.log(err);
				callback(err);	
			}
			else{
				// console.log(data);
				 // last item
				fs.writeFile('logs/'+shortKey, data.Body, function(err) {
				    if(err) {
				        console.log(err);
				        callback(err);
				    }
				    else{
					    console.log("  - new file downloaded");
					    callback(null);
				    }
				}); 
			}
		});
	}
}

var sendLinesToFilebeat = function(file,callback){
	var lineReader = readline.createInterface({
	  input: fs.createReadStream(file).pipe(zlib.createGunzip())
	});

	var n = 0;
	lineReader.on('line',function(line,lineCount,byteCount){
		n += 1
		line=transformLine(line);
		fs.appendFileSync("./nodejs.log", line.toString() + "\n");
	});
	lineReader.on("error",function(e){
		callback(e);
	});
	lineReader.on("close",function(){
		console.log('  - '+n + ' lines send to filebeat');
		stats.lines_count+=n;
		stats.file_end_pos[file]=stats.lines_count;
		callback(null);
	});
}


/**
 * wrapper function that defines everything that needs to be done to a relevant log file
 * @param  {[type]}   key      full key
 * @param  {Function} callback [description]
 */
var processOneFile = function(key,callback){
	var shortKey=getShortKey(key);

	async.series([
		async.apply(downloadOneFile,config.bucket,key),
		async.apply(sendLinesToFilebeat,'logs/'+shortKey),

	],function(err,result){
		// console.log(' - this file is processed');
		callback(err);
	});
}




var queryElasticSearch=function(start,end,callback){
	var request = require("request");
	var body = {
		"size": 1000,
		"query": {
			"bool": {
				"must": [
					{
						"query_string": {
							"analyze_wildcard": true,
							"query": "log_type:'"+config.contains+"'"
						}
					},
					{
						"range": {
							"log_timestamp": {
								"gte": start,
								// "gte": new Date(config.start_date).getTime(),
								"lt": end,
								// "lte": new Date(config.end_date).getTime(),
								"format": "epoch_millis"
							}
						}
					}
				],
				"must_not": []
			}
		},
		"_source": {
			"excludes": []
		}
	};
	// console.log(JSON.stringify(body));
	var options = { method: 'POST',
		url: 'https://search-highlyreco-5-3-ghskwsnysgc4nqmuevh373svqe.us-east-1.es.amazonaws.com/c-rotated-logs/_search',
		headers:{ 'postman-token': '3b91b7a4-fc2a-d5eb-7534-d439b163ee6a',
		'cache-control': 'no-cache' },
		body: JSON.stringify(body)
	};
	console.log(' - Fetching data from elastic search');
	request(options, function (error, response, body) {
		if (error) callback(error);
		var body = JSON.parse(body);
		// console.log(body);
		// console.log('\n\n\n=====');
		// console.log(body.hits);
		body.hits.hits.forEach(function(hit){
			relevantKeys.push(hit._source);
		});
		console.log(' - This day has - '+relevantKeys.length+' logs');
		callback(error);
	});
}

/**
 * this is process all keys one by one in series or parallel
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
var processAllRelevantKeys=function(callback){
	console.log(' - Start processing all logs');
	async.eachSeries(relevantKeys,function(item,callback){
				// console.log(item);
		var temp = item.Key.split('/');
		var instance_id=temp[temp.length-2];

		// console.log(i+') '+getShortKey(item.Key)+' (Size = '+item.Size+', instance_id = '+instance_id+')');
		// i++;
		processOneFile(item.Key,callback);
		
		// callback(null);
	},function(err,results){
		console.log(' - Processing all logs complete');
		callback('done');
	});
}

/**
 * this process one days worth of logs
 * @param  {[type]}   day      date obj - start of the day
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
var processLogsForADay = function(day,callback){
	console.log('\n\n');
	console.log('----------------------------------------------------------------');
	console.log('Starting to process logs for - '+day.toISOString());
	console.log('----------------------------------------------------------------');
	var start=day.getTime();
	var end = day.getTime()+24*60*60*1000; // add one day
	relevantKeys=[];
	async.series([
		async.apply(queryElasticSearch,start,end),
		processAllRelevantKeys,
	],function(err,results){
		console.log('----------------------------------------------------------------');
		console.log(relevantKeys.length+' log files for '+day.toISOString()+' processed');
		console.log('----------------------------------------------------------------');
		callback(err);
	});
}



var processManyDaysLogs = function(){
	console.log('Start');
	var day = new Date(config.start_date);
	async.forever(function(next){
		// console.log(day.toISOString());
		if(day.getTime() == new Date(config.end_date).getTime())
			next('done');
		else{
			// console.log(day.toISOString());
			processLogsForADay(day,function(err,result){
				day = new Date(day.getTime()+24*60*60*1000);
				next(null);
			});
		}
	},function(err,result){
		if(err && err!='done'){
			console.log('*&*(%*&%$&^$# - we go an error');
			console.log(err);
		}
		else{
			console.log('\n\n');
			console.log('================================================================');
			console.log('Moved all logs from '+config.start_date+' to '+config.end_date+' to Filebeat');
			console.log('Lines count - '+stats.lines_count);
			console.log('================================================================');

		}

	});
}
processManyDaysLogs();


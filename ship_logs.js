/**
 * This file is used to retrieve logs from s3. 
 * 
 */

var AWS = require('aws-sdk');
var async = require('async');
var fs = require('fs');
var zlib     = require('zlib');
var readline = require('readline');
// var s3 = new AWS.S3();

var config = require('./config.js')
var s3 = new AWS.S3({accessKeyId:config.accessKeyId,secretAccessKey:config.secretAccessKey});
if(fs.existsSync('./nodejs.log'))
	fs.unlinkSync("./nodejs.log"); // delete the file, to start with and empty file

/**
 * the params required for getting objects from s3
 * @type {Object}
 */
var params = {
	Bucket:config.bucket,
	MaxKeys: config.max_keys,
	// Prefix:'resources/environments/logs/publish/e-gkbr8nwcv4/i-000649c815461185a'
	Prefix:config.prefix+'/'+config.env_id,
};
if(config.start_after)
	params.StartAfter=config.prefix+'/'+config.env_id+'/'+config.start_after;


/**
 * Get all the log files
 * @return {[type]} [description]
 */
var getAllFiles=function(){

	var i = 1;
	async.forever(function(next){
		console.log('\n\n\n==================================================');
		console.log('Getting a set of keys, iteration - '+i);
		i++;
		getOneSetOfObjects2(params,next);
	},function(err,result){
		if(err)
			console.log(err);
		console.log('done');
	});
	// 
}



/**
 * This gets one set of objects from an AWS bucket
 * and then proceeds to download them to disk
 * @param  {[type]}   params   The parameters needed by s3
 * @param  {Function} callback [description]
 */
var getOneSetOfObjects = function(params,callback){
	s3.listObjectsV2(params,function(err,data){
		if(err)
			callback(err);
		else{
			console.log(data.KeyCount);
			var i=1;
			async.eachLimit(data.Contents,1,function(item,callback){
				// console.log(item);
				var temp = item.Key.split('/');
				var instance_id=temp[temp.length-2];

				console.log(i+' - '+temp[temp.length-1]+' (Size = '+item.Size+', instance_id = '+instance_id+')');
				i++;
				// downloadOneFile(config.bucket,item.Key,callback);
				
				callback(null);
			},function(err,results){
				console.log(err);
				if(data.MaxKeys==data.KeyCount){
					// console.log('there is more');

					params.StartAfter=data.Contents[data.KeyCount-1].Key;
					console.log('----------------------------------------------');
					console.log('Got objects till - '+params.StartAfter);
					console.log('----------------------------------------------');
					callback(err);
				}
				else
					callback('done');
			});
		}
	});
}

var relevantKeys=[];
var getOneSetOfObjects2 = function(params,callback){
	s3.listObjectsV2(params,function(err,data){
		if(err)
			callback(err);
		else{
			// console.log(data.KeyCount);
			relevantKeys=[];
			data.Contents.forEach(function(item,i){
				if(item.Key.indexOf(config.contains)>=0){
					var temp = item.Key.split('-')
					var timestamp=parseInt(temp[temp.length-1].substring(0,10));
					var start_date=new Date(config.start_date).getTime()/1000;
					var end_date=new Date(config.end_date).getTime()/1000;
					if(timestamp>start_date && timestamp<end_date){
						// console.log(start_date+' > '+timestamp+' > '+end_date);
						relevantKeys.push(item);
					}
				}
			});

			// console.log(relevantKeys);

			var i=1;
			async.eachLimit(relevantKeys,1,function(item,callback){
				// console.log(item);
				var temp = item.Key.split('/');
				var instance_id=temp[temp.length-2];

				console.log(i+') '+getShortKey(item.Key)+' (Size = '+item.Size+', instance_id = '+instance_id+')');
				i++;
				processOneFile(item.Key,callback);
				
				// callback(null);
			},function(err,results){
				console.log(err);
				if(data.MaxKeys==data.KeyCount){
					// console.log('there is more');

					params.StartAfter=data.Contents[data.KeyCount-1].Key;
					console.log('----------------------------------------------');
					console.log('KeyCount = '+data.KeyCount+', relevantKeys = '+relevantKeys.length);
					console.log('Got objects till - '+getShortKey(params.StartAfter));
					console.log('----------------------------------------------');
					callback(err);
				}
				else
					callback('done');
			});
		}
	});
}

getAllFiles();
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
		console.log(' - '+n + ' lines send to filebeat');
		callback(null);
	});
}
/**
 * Any transformations that you want to do to this line
 * @param  {[type]} line [description]
 * @return {[type]}      [description]
 */
var transformLine = function(line){
	line = line.replace('"app_env":"production"','"app_env":"pro2"');
	line = line.replace('"app_env":"prod"','"app_env":"pro2"');
	line = line.replace('"app_env":"development"','"app_env":"dev2"');
	return line;
}
var deleteFile = function(file,callback){
	fs.unlinkSync(file);
	// callback();
}

// deleteFile('./nodejs.log');
// sendLinesToFilebeat('logs/_var_log_nodejs_rotated_nodejs.log-1486638061.gz');
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
    console.log(' - file already downloaded');
		callback(null);
	}
	else{
		console.log(' - starting to download a file now');
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
					    console.log(" - new file downloaded");
					    callback(null);
				    }
				}); 
			}
		});
	}
}
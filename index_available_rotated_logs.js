/**
 * this script is to index all the available rotated logs to elastic search
 */



var config = require('./config.js');
var AWS = require('aws-sdk');
var elasticsearch = require('elasticsearch');
var async = require('async');
var s3 = new AWS.S3({accessKeyId:config.accessKeyId,secretAccessKey:config.secretAccessKey});
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


var client = new elasticsearch.Client({
	host: config.elasticsearch.protocol +
		'://' +
		// gooduser:secretpasswor
		// config.elasticsearch.user +
		// ':' +
		// config.elasticsearch.password +
		// '@' +
		config.elasticsearch.host +
		':' +
		config.elasticsearch.port
});

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
		getOneSetOfObjects(params,next);
	},function(err,result){
		if(err)
			console.log(err);
		console.log('done');
	});
	// 
}
/**
 * This gets one set of objects from an AWS bucket
 * and then proceeds to ship the object to elastic seach
 * @param  {[type]}   params   The parameters needed by s3
 * @param  {Function} callback [description]
 */
var getOneSetOfObjects = function(params,callback){
	s3.listObjectsV2(params,function(err,data){
		if(err)
			callback(err);
		else{
			console.log(data.KeyCount);

			var batch_size = 50;
			var oneBatchOfItems=[];
			var batchesOfItems=[];
			data.Contents.forEach(function(item,i){
				if(i%batch_size==0 && oneBatchOfItems.length!=0){
					batchesOfItems.push(oneBatchOfItems);
					oneBatchOfItems=[];
				}
				oneBatchOfItems.push(item);
			});
			batchesOfItems.push(oneBatchOfItems); // pushing the last batch
			// console.log(batchesOfItems);
			// console.log(batchesOfItems.length);
			// console.log(batchesOfItems[0].length);
			
			async.eachLimit(batchesOfItems,5,function(oneBatchOfItems,callback){

				batchUploadToES(oneBatchOfItems,callback);
				// callback(null);
			},function(err,results){
				if(err)
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

var updateES=function(callback){
	callback(null);
}

var batchUploadToES = function(items,callback){
	var body=[];
	items.forEach(function(item){
		var temp = item.Key.split('/');
		item.env_id = temp[4]; // e-gkbr8nwcv4
		item.instance_id = temp[5]; // i-000f58cf70c1cf8fc
		item.file_name = temp[6]; // _var_log_nginx_rotated_access.log-1496851261.gz
		item.log_type = temp[6].split('-')[0]; // _var_log_nginx_rotated_access.log
		var ts = temp[6].split('-')[1].split('.')[0] // 1496851261
		item.log_timestamp = new Date(parseInt(ts)*1000).toISOString(); // 2017-06-07T16:01:01.000Z
		
		var action={
			index:  { _index: 'c-rotated-logs', _type: 'rotated_log_files', _id: item.ETag }
		};
		body.push(action);
		body.push(item);
	});
	// console.log('\n\n\n\n*******');
	// console.log(body);
	client.bulk({body:body},function(err,response){
		if(err){
			console.log('error happened');
			console.log('elasticsearch error ' + err);
			callback(err);
		}
		else{
			console.log(' - uploaded '+items.length+' items to ES');   
			callback(null);
		}
	});        
}


var main = function(){
	getAllFiles();
};
main();

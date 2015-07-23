/*
DIREKTSPEED LTD Couchbase backend.
*/

"use strict";

var couchbase = require('couchbase');
var _ = require('lodash');
var Promise = require('bluebird');
Promise.promisifyAll(couchbase);

/**
	Instantiates a couchbase backend.

	opts: {
		host: 'localhost',
		opts: {
			port: 6739,
			prefix: '',
			bucket: 'default'
		}
	}
*/
function CouchbaseBackend(hostname, opts)
{
	if(!(this instanceof CouchbaseBackend)){
		return new CouchbaseBackend(hostname, opts);
	}

	opts = opts || {};
	opts.bucket = opts.bucket || 'default';
	port = opts.port || 8091;
	hostname = hostname || 'localhost';

	// open cluster, 
	this.cluster = new couchbase.Cluster(hostname + ':' + port);
        this.ViewQuery = couchbase.ViewQuery;
        this.bucket = this.cluster.openBucket(opts.bucket);
}

/**
	Returns a array with all the 
	registered services and removes the expired ones.
*/
CouchbaseBackend.prototype.getServices = function(){
	var cluster = this.cluster;
	var ViewQuery = this.ViewQuery;
	var bucket = this.bucket;
	
	//
	// Get all members in the service set.
	//
	return bucket.query(ViewQuery.from('vhosts', 'by_id').stale(ViewQuery.Update.BEFORE), function(err, values) {
			    
			    // we will fetch all the titles documents based on its id.
			    // console.log(values);
			    var keys = _.pluck(values, 'id');
			    // console.log(typeof keys);
			    // console.log(keys);
			    bucket.getMulti( keys, function(err, results) {  	
	     		    // console.log(results);
				return results;
				/*	     		     
		     		i = 0;
				
		     		_.pluck(results, 'value').forEach(function(entry) {
    					// console.log('{\n "id": "' + JSON.parse(entry).uid + '", \n "publish_date": "' + c + '" \n },');
    					i++
    					if (!(i < _.size(results))) console.log('ready');
					});
				*/
		
			     });
			});
	
	})
}

/**
	Returns array with infos whats in array but not in couchbase 
	expects to get. object or array with keys that are currently routed
*/
CouchbaseBackend.prototype.getClearServices = function(current_services){
	var cluster = this.cluster;
	var bucket = this.bucket;
	
	return Promise.resolve(only_the_keys(current_services)
	.then(	
		function(current_services) {
			return bucket.getMulti( current_services, function(err, results) {  	
				return results;
	       		});	
		}
	);
}

/**
	gets array of current routes and returns only the keys as array so you can use 
	expects to get. object or array with keys that are currently routed
*/
function only_the_keys(current_services){
	// for each current_services clear all infos out so that only the keys remain and
	// return a promis with that infos
	return Promise.resolve(function(current_services) {
		// for each key store same key we use i loop to overwrite keys in object
		return current_services;
	});
}

module.exports = CouchbaseBackend;

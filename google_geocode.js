var GKEY = ""; // google API key

const promise = require('bluebird'),
      request = require('request'),
      pgp = require('pg-promise')({promiseLib: promise, noWarnings: true});

const connection = ""; // postgres connection string

const table = "", limit = 50;

const db = pgp(connection);


// select rows with empty coordinates
function select_rows(limit, callback){
    
    let query = "select id, address from " + table + " where lat is null or lng is null order by id limit " + limit + ";";
    
    db.any(query)
    .then(function(data){
        let arr = []; len = data.length;
        
        for(let i=0; i<len; i++){
            arr.push({"id": data[i].id, "address": data[i].address});
        }
        
        if(typeof(callback)==='function'){
            callback(arr);
        } else {
            console.log('No callback function defined.');
            console.log(arr); 
        }
    })
    .catch(function(e){
        console.log(e);
    });
}

// create array of request urls
function construct_URLs(items){
    var base_url = 'https://maps.googleapis.com/maps/api/geocode/json?',
        auth = "&key=" + GKEY + '&address=',
        len = items.length, 
        urls = [];
    
    for(let i=0; i<len; i++){
        let address = (items[i].address).replace(new RegExp(" ", "g"), "+"),
            reqURL = base_url + auth + address,
            url = {
                "id": items[i].id,
                "url": reqURL
            };
        urls.push(url);
        
        //console.log(urls);
        console.log('url array constructed');
        
        try {
            if(urls.length){
                fire_requests(urls);
            } else {
                console.log('All dataset geocoded.');
            }
        } catch(e){
            console.log(e);
        }
    }
}


function fire_requests(urls){
    console.log('starting requests');
    
    urls.map(function(url){
        request(url.url, function(error, response,body){
            if(!error){
                try {
                    var json = JSON.parse(body) || undefined;
                    
                    if(!!json.results[0]){
                        
                        let lat = json.results[0].geometry.location.lat,
                            lng = json.results[0].geometry.location.lng,
                            item = {
                                "id": url.id,
                                "lat": lat,
                                "lng": lng
                            };
                        
                        console.log('successfuly geocoded: ' + JSON.stringify(item));
                        
                        update_row(item);
                    
                    } else {
                        console.log('zero results for ' + url.url + " " + url.id);
                    }
                } catch (e){
                    console.log(e);
                }
            } else {
                console.log(body);
            }
        });
    });
}

// update row with coordinates
function update_row(item){
    let id = item.id,
        lat = item.lat,
        lng = item.lng;
    
    let update = "update " + table + " set lat = " + lat 
    + ", lng = " + lng + " where id = " + id + ";";  
    
    console.log(update);
    
    db.any(update).then(function(){
        console.log(id + ' updated.');
    }).catch(function(err){
        console.log(err);
    });
}


/* Table create statement

create table data_to_geocode
(
	id serial not null
		constraint aus_retail_id_pk
			primary key,
	address text,
	lat double precision,
	lng double precision
);

create index gix_data_to_geocode_geom
	on data_to_geocode using GIST(geom);

-- trim whitespace, new lines etc. from address

*/

/* execution */
function geocode(){
    select_rows(limit, construct_URLs);
    setTimeout(function(){
        select_rows(limit, construct_URLs);
    }, 1000*61);
}

// geocode();

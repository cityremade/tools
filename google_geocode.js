/*const $ = {
    connection: "",
    gkey: "",
    _table: "",
    limit: 50,
    item: [],
    address_field: "" // address columns or concatenation
}*/

const promise = require('bluebird'),
      request = require('request'),
      pgp = require('pg-promise')({promiseLib: promise, noWarnings: true});

module.exports = function($){
    
    $.db = pgp($.connection);
    
    function select(callback){
        
        let addr;
        
        $.urls = [];
        
        $.id_alias ? $.id = $.id_alias : $.id = "id";
        
        if($.address_field) {
            addr = ", " + $.address_field;
        } else {
            addr = ", address";
        }
        
        // added deliver for pizza hut table
        let query = "select " + $.id + " as id " + addr + " as address from " + $._table + " where lat is null or lng is null order by id limit " + $.limit + ";";
        
        //console.log(query);
        
        $.db.any(query)
            .then(function(data){
            let len = data.length;
            
            for(let i=0; i<len; i++){
                $.urls.push({"id": data[i].id, "address": data[i].address});
            }
            
            if(typeof(callback)==='function'){
                callback($.urls);
            } else {
                console.log('No callback function defined.');
                console.log($.urls); 
            }
        })
            .catch(function(e){
            console.log($.id + ": " + e);
            //console.log(e);
        });
    }
    
    function construct_URLs(items){
        var base_url = 'https://maps.googleapis.com/maps/api/geocode/json?',
            auth = "&key=" + $.gkey + '&address=',
            len = items.length, 
            urls = [];
        
        for(let i=0; i<len; i++){
            let address = (items[i].address.replace(new RegExp(", ", "g"), "+"));
                address = encodeURIComponent(address);
                reqURL = base_url + auth + address;
                url = {
                    "id": items[i].id,
                    "url": reqURL
                };
            
            urls.push(url);
        }
            //console.log(urls);
        console.log('url array of ' + urls.length + ' constructed');
        
        try {
            if(urls.length){
                fire_requests(urls);
            } else {
                console.log('All dataset geocoded.');
                return;
            }
        } catch(e){
            console.log(e);
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
                            update_row({"id": url.id, "lat": -1, "lng": -1});
                        }
                    } catch (e){
                        update_row({"id": url.id, "lat": -1, "lng": -1});
                        console.log(url.id + ": " + e);
                    }
                } else {
                    //console.log(body);
                }
            });
        });
    }
    
    // update row with coordinates
    function update_row(item){
        let id = item.id,
            lat = item.lat,
            lng = item.lng;
        
        let update = "update " + $._table + " set lat = " + lat 
        + ", lng = " + lng + " where " + $.id + " = " + id + ";";  
        
        console.log(update);
        
        $.db.any(update).then(function(){
            console.log(id + ' updated.');
        }).catch(function(err){
            console.log(err);
        });
    }
    
    /* execution */
    /*function geocode(){
        select(construct_URLs);
        //if($.urls.length) {
            setTimeout(function(){
                select(construct_URLs);
            }, 1000*61);
        //}
    }*/
    
    function geocode(){
        select(construct_URLs);
        setInterval(function(){
            select(construct_URLs);
        }, 1000*61);
        //if(!$.urls.length) clearInterval(interval);
    }
    
    return {
        geocode: geocode
    }
}

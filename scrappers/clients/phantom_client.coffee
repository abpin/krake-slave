# @Description : This class interfaces between the local PhantomWeb server and Krake engine

http = require 'http'
KSON = require 'kson'

class PhantomClient

  # @Description : sets up the HTTP communication protocol to the PhantomWebserver
  constructor: (@settings)->

  # @Description : get results from a page given a single level krake definition
  # @param : krake_query:Object
  # @param : callback:function(status:String, results:resultsObj)
  #      resultsObj:Object
  #        result_rows:Array
  #          result_row:Object
  #            attribute1:String value1 - based on required_attribute
  #            attribute2:String value2 - based on required_attribute
  #            ...
  #        next_page:String — value to next page href
  #        logs:Array
  #          log_mesage1:String, ...  
  getResults : (krake_query, callback)->
  
    # ensures support for UTF8
    krake_string = encodeURIComponent(JSON.stringify(krake_query))
    post_options =
      host: @settings.host
      port: @settings.port
      path: @settings.path
      method: 'POST'
      headers: 
        'Content-Length': krake_string.length
    
    post_req = http.request post_options, (res)=>
      res.setEncoding('utf8')
      
      consolidatedData = ""
      res.on 'data', (raw_data)=>
        consolidatedData += raw_data
      
      res.on 'end', ()=>
        response_obj = JSON.parse consolidatedData
        response_obj.message = response_obj.message || {}
        callback && callback response_obj.status, response_obj.message

    # write parameters to post body
    post_req.write(krake_string)
    post_req.end()
  
module.exports = PhantomClient

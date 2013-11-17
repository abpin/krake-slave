# Only works when header settings not added during 
PhantomClient = require '../../../scrappers/clients/phantom_client'
KSON = require 'kson'
jasmine.getEnv().defaultTimeoutInterval = 30000
fs = require 'fs'

global.CONFIG = null;
global.ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
try 
  CONFIG = KSON.parse(fs.readFileSync('./config/config.js').toString())[ENV];
catch error
  console.log 'cannot parse config.js : %s', error
  process.exit(1)

settings = 
  host : CONFIG.phantomServer.host
  port : CONFIG.phantomServer.port
  path : CONFIG.phantomServer.path

pc = new PhantomClient settings

describe "PhantomClient test to ensure xpath has not error", ()->
  it "should respond with success and a valid email ", (done)->

    krake_definition =
      origin_url : 'http://tw.user.mall.yahoo.com/booth/view/stIntroMgt?sid=icewoods'
      columns : [{
        col_name : 'email addresses'
        xpath : '//*[@id="ypsint"]/div[2]/div/div/div/div/div[3]'
        required_attribute : 'email'
      }]
    
    pc.getResults krake_definition, (status, results)->
      expect(status).toEqual "success"
      expect(typeof results).toBe "object"
      done()



describe "PhantomClient Test to ensure dom_query has no error", ()->
  it "Checks to make sure Gary teh is returned", (done)->

    krake_definition =
      "origin_url": "http://www.linkedin.com/in/garyjob",
      "columns": [{
          "col_name": "name",
          "dom_query": ".full-name"
          "required_attribute" : 'innerText'
        }]

    pc.getResults krake_definition, (status, results)->
      expect(status).toEqual "success"      
      expect(typeof results).toBe "object"
      done()
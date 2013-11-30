GeolocationScrapper = require '../../scrappers/geolocation_scrapper'
KSON = require 'kson'
fs = require 'fs'

describe "ensure valid email addresses are returned when required_attribute = email ", ()->
  it "should respond with a set of Geolocation ", (done)->

    task_info_obj =
      task_id: 'DEMO1385561284067'
      columns: [{ 
        col_name: 'store location'
        xpath: '/html[1]/body[1]/table/tbody[1]/tr[1]/td[1]/table[1]/tbody[1]/tr[4]/td[1]/table[1]/tbody[1]/tr[1]/td[1]/table[1]/tbody[1]/tr[4]/td[1]/table[1]/tbody[1]/tr[1]/td[1]/table/tbody[1]/tr[2]/td[2]'
        required_attribute: 'address' 
      }]
      data: 
        origin_pattern: 'http://www.coldstorage.com.sg/corporate/Public/corporate_storelocations.html#coldstorage'
        origin_url: 'http://www.coldstorage.com.sg/corporate/Public/corporate_storelocations.html#coldstorage'
        'store location': 'Singapore 120429'
      task_type: 'geolocation scrape'
    
    gs = new GeolocationScrapper task_info_obj
    gs.processTask (scenario, geo_results)=>
      expect(geo_results.length).toEqual 1
      expect(typeof geo_results[0]["store location_lat"]).toBe "number"
      done()



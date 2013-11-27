# Only works when header settings not added during 
ListingPageScrapper = require '../../scrappers/listing_page_scrapper'
KSON = require 'kson'
fs = require 'fs'
jasmine.getEnv().defaultTimeoutInterval = 30000

global.CONFIG = {
  "redis": {
    "host": "localhost",
    "port": "6379",
    "scrapeMode": "depth",
    "queueName": "queue1"
  },
  "usageServer" : "http://localhost:9805",
  "publishingServer" : "http://localhost:9806",
  "phoenixServer" : {
    "url" : "http://localhost:9801",
    "pageCrawlLimit" : 100,
  },
  "phantomServer" : {
    "host" : "localhost",
    "port" : "9701",
    "path" : "/extract",
    "timeout" : 120      
  },
  "canRotateIP" : "cannot"
}

describe "ensure valid email addresses are returned when required_attribute = email ", ()->
  it "should respond with success and a valid email ", (done)->

    krake_definition =
      origin_url : 'http://tw.user.mall.yahoo.com/booth/view/stIntroMgt?sid=icewoods'
      columns : [{
        col_name : 'email addresses'
        xpath : '//*[@id="ypsint"]/div[2]/div/div/div/div/div[3]'
        required_attribute : 'email'
      }]
    
    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['email addresses']).toEqual "sms01@playwoods.com,serv001@icewoods.com,serv001@icewoods.com"
      ,()=>
        done()
          

          
describe "ensure valid phone numbers when required_attribute = phone ", ()->
  it "should respond with success and a valid email ", (done)->

    krake_definition =
      origin_url : 'http://tw.user.mall.yahoo.com/booth/view/stIntroMgt?sid=icewoods'
      columns : [{
        col_name : 'phone numbers'
        xpath : '//*[@id="ypsint"]/div[2]/div/div/div/div/div[3]'
        required_attribute : 'phone'
      }]

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['phone numbers']).toEqual "(02)2528-2825,(02)2733-7333"
      ,()=>
        done()



describe "ensure public profile linkedin name is successfully returned using Xpath", ()->
  it "Checks to make sure Gary teh is returned", (done)->

    krake_definition =
      "origin_url": "http://www.linkedin.com/in/garyjob",
      "columns": [{
          "col_name": "name",
          "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[1]/div[1]/h1[1]/span[1]/span[1]"
          "required_attribute" : 'innerText'
        }]
    
    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition    
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['name']).toEqual "Gary Teh"
      ,()=>
        done()



describe "ensure public profile linkedin name is successfully returned using dom_query", ()->
  it "Checks to make sure Gary teh is returned", (done)->

    krake_definition =
      "origin_url": "http://www.linkedin.com/in/garyjob",
      "columns": [{
          "col_name": "name",
          "dom_query": ".full-name"
          "required_attribute" : 'innerText'
        }]

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['name']).toEqual "Gary Teh"
      ,()=>
        done()



describe "ensure nothing is returned if required_attribute phone on no phone number dom", ()->
  it "Checks to make sure returns nothing", (done)->

    krake_definition =
      "origin_url": "http://www.qoo10.sg/gmkt.inc/MiniShop/ShopInfo.aspx?sell_cust_no=xBtElNUBeN%2fGX13rU7PvQg%3d%3d&global_yn=N",
      "columns": [{
          "col_name": "phone",
          "dom_query": ".g_seller_content"
          "required_attribute" : 'phone'
        }]

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition    
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['phone']).toEqual ""
      ,()=>
        done()



describe "ensure valid email addresses are returned when required_attribute = email ", ()->
  it "should respond with success and a valid email ", (done)->

    krake_definition = {
      "origin_url" : "http://www.qoo10.sg/gmkt.inc/MiniShop/ShopInfo.aspx?sell_cust_no=dszaNfJNyEbbJhlcgbsFJA%3d%3d&global_yn=N",
      "columns": [
        {
            "col_name": "address",
            "dom_query": "h2:contains('Shop Info')+ul li:contains('Address')"
        },
        {
            "col_name": "person in charge",
            "dom_query": "h2:contains('Shop Info')+ul li:contains('Management Staff')"
        },
        {
            "col_name": "email",
            "dom_query": "h2:contains('Shop Info')+ul li:contains('E-mail')",
            "required_attribute": "email"
        },
        {
            "col_name": "phone",
            "dom_query": "h2:contains('Shop Info')+ul li:contains('Contact No')",
            "required_attribute": "phone"
        },          
        {
            "col_name": "website",
            "dom_query": "h2:contains('Shop Info')+ul li:contains('Seller shop address')"
        },
        {
            "col_name": "seller level",
            "dom_query": ".icon_grade",
            "required_attribute" : "alt"
         },
         {
             "col_name": "registered item",
             "dom_query": "span:contains('All registered item')",
             "regex_pattern" : /[0-9]+/
          },           
          {
              "col_name": "seller service rating",
              "dom_query": ".mshop_rate dfn",
              "required_attribute": "style",
              "regex_pattern": /[0-9]+/gi
          }
      ]
    }

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['seller level']).toEqual "POWER"
          expect(interim_results[0]['registered item']).toEqual "131"
          expect(interim_results[0]['phone']).toEqual "+82-051-638-3422"
          expect(interim_results[0]['email']).toEqual "amazones7870@gmail.com"
      ,()=>
        done()



        describe "ensure valid phone numbers when required_attribute = phone ", ()->
          it "should respond with success and a valid email ", (done)->

            krake_definition =
              origin_url : 'http://tw.user.mall.yahoo.com/booth/view/stIntroMgt?sid=icewoods'
              columns : [{
                col_name : 'phone numbers'
                xpath : '//*[@id="ypsint"]/div[2]/div/div/div/div/div[3]'
                required_attribute : 'phone'
              }]

            settings = {}
            settings.config = CONFIG
            settings.task_info_obj = krake_definition
            ls = new ListingPageScrapper settings
            ls.processTask (scenario, interim_results)->
                if scenario == "listings with no need to scrape detailed pages extracted"
                  expect(typeof interim_results).toBe "object"
                  expect(interim_results[0]['phone numbers']).toEqual "(02)2528-2825,(02)2733-7333"
              ,()=>
                done()



describe "ensure nothing is returned if required_attribute phone on no phone number dom", ()->
  it "Checks to make sure returns nothing", (done)->

    krake_definition =
      "origin_url": "http://www.qoo10.sg/gmkt.inc/MiniShop/ShopInfo.aspx?sell_cust_no=xBtElNUBeN%2fGX13rU7PvQg%3d%3d&global_yn=N",
      "columns": [{
          "col_name": "phone",
          "dom_query": ".g_seller_content"
          "required_attribute" : 'phone'
        }]

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition    
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(interim_results[0]['phone']).toEqual ""
      ,()=>
        done()



describe "checks definition against yelp ", ()->
  it "should respond with success and a valid email ", (done)->

    krake_definition =
      "origin_url": "http://yelp.com/biz/gary-danko-san-francisco",
      "wait" : 10000,
      "columns": [
          {
              "col_name": "rating",
              "dom_query": "#bizInfoHeader .rating .star-img",
              "required_attribute": "title",
              "regex_pattern" : /[0-9]+/
          },
          {
              "col_name": "opentable_exists",
              "dom_query": '#opentable-search-form:contains("Make a Reservation") legend'
          },
          {
              "col_name": "address",
              "dom_query": "address",
              "required_attribute" : "innerText"
          },
          {
              "col_name": "num_reviews",
              "dom_query": ".review-count:first",
              "regex_pattern" : /[0-9]+/              
          },
          {
              "col_name": "Price",
              "dom_query": "#price_tip"
          },
          {
              "col_name": "Phone",
              "dom_query": "#bizPhone"
          },
          {
              "col_name": "Website",
              "dom_query": "#bizUrl a"
          }
      ]

    settings = {}
    settings.config = CONFIG
    settings.task_info_obj = krake_definition
    ls = new ListingPageScrapper settings
    ls.processTask (scenario, interim_results)->
        if scenario == "listings with no need to scrape detailed pages extracted"
          expect(typeof interim_results).toBe "object"
          expect(typeof interim_results[0]['Website']).toBe "string"
          expect(typeof interim_results[0]['num_reviews']).toBe "string"
          expect(typeof interim_results[0]['rating']).toBe "string"
      ,()=>
        done()



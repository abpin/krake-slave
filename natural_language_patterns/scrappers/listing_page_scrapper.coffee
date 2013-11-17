# @Description: this class extends the scrapper object and is specialized for extracting a list of data entries 
#   as well as the next page url

# Dependencies
fs = require 'fs'
kson = require 'kson'
async = require 'async'
DataTransformer = require("krake-toolkit").query.data_transformer
PhantomClient = require './clients/phantom_client'

class ListingPageScrapper

  # @Description: default constructor
  # @param: 
  #   settings:object
  #     config:object
  #     task_option_obj :
  #       origin_url : 'http://www.mdscollections.com/cat_mds_accessories17.cfm'
  #       columns : [{
  #         col_name : 'title'
  #         dom_query : '.listing_product_name' 
  #       }, { 
  #         col_name : 'price'
  #         dom_query : '.listing_price' 
  #       }, { 
  #         col_name : 'detailed_page_href'
  #         dom_query : '.listing_product_name'
  #         required_attribute : 'href'
  #         options
  #           columns : [{
  #             col_name : 'description'
  #             dom_query : '.tabfield18504'
  #           }]
  #     }]
  #       next_page :
  #         dom_query : '.listing_next_page'
  # @param: initial_callback:function()
  constructor: (@settings)->
    @options = @settings.task_info_obj
    @prior_data_obj = @options.data || {}
    @scraper_client = new PhantomClient @settings.config.phantomServer

  

  # @Description: The default interface external processes use to interact with a Scrapper object.
  # @param: interim_callback:function(scenario:string, data_obj:object)
  #   Current scenarios:
  #     listings with need to scrape detailed pages extracted
  #     listings with no need to scrape detailed pages extracted
  # @param: offsprings_callback:function()    
  # @param: final_callback:function()  
  processTask : (@interim_callback, @final_callback)->
    @options.url_to_process = @options.origin_url
    @processPage()

  
  
  # @Description: extracts listing in current page and finds url for next page
  # @param: interim_callback:function(scenario:string, data_obj:object)
  #   Current scenarios:
  #     listings with need to scrape detailed pages extracted
  #     listings with no need to scrape detailed pages extracted
  # @param: final_callback:function()
  processPage : ()->
    cookies_array = @options.cookies || []  
        
    @scraper_client.getResults @options, (status, results)=>
      # Handle results return
      results.result_rows = results.result_rows || []
      results.result_rows = results.result_rows.map (curr_row)=>
      
        transformed_row = {}
        Object.keys(curr_row).forEach (col_name)=>
          curr_col = @options.columns.filter ( columns )=>
            columns.col_name == col_name
          dt = new DataTransformer curr_row[col_name], curr_col[0]
          transformed_row[col_name] = dt.getValue()
        
        @merge_results @options.data, transformed_row
        
      @returnListItems results.result_rows
      # Handle pagination
      results.next_page && @interim_callback 'next page href extracted', results.next_page      
      @final_callback && @final_callback()
      @end()
    
  
  
  # @Description : takes the list of items scraped initially from page thereafter determine which 
  #   the scenario to call when doing the @interim_callback
  # @param : list_items: array[object_1, object_2...]
  # @param : columns: array[col_name_1, col_name_2...]  
  # @param : results_callback: function(scenario:string, list_items) 
  returnListItems : (list_items, columns, results_callback)->
    
    if !list_items || list_items.length == 0
      results_callback && results_callback 'no new data extracted', false
      
    else
        
      results_callback = results_callback || @interim_callback
      columns = columns || (@options && @options.columns)
      
      deep_dived = false
      # handling geolocation dives
      if @hasAddressAttributes(columns)
        deep_dived = true
        results_callback && results_callback 'listings with need to scrape geolocation information', list_items

      # handling sub page dives via href
      if @hasNestedColumns(columns)
        deep_dived = true
        results_callback && results_callback 'listings with need to scrape detailed pages extracted', list_items

      if !deep_dived
        results_callback && results_callback 'listings with no need to scrape detailed pages extracted', list_items  
  
  
  
  # @Description: removes all the variables that were set for this job as well as kill phantomJs background process
  end : ()->
    @options = false
    @interim_callback = false
    @final_callback = false
   


  # @Description: checks if any of the current top level columns is attempting to get special attribute - address
  # @return has_address_attributes:boolean
  hasAddressAttributes : (columns)->
    @options.columns = @options.columns || []
    address_cols = @options.columns.filter ( column )=>
      column.required_attribute && column.required_attribute == 'address'
    address_cols && address_cols.length > 0



  # @Description: gets all the current top level columns with address options
  # @return address_cols:array 
  #   E.g.
  #     [{ 
  #       col_name : 'office_address'
  #       dom_query : '.listing_address'
  #       required_attribute : 'address'
  #     }, { 
  #       col_name : 'warehouse_address'
  #       dom_query : '.listing_warehouse'
  #       required_attribute : 'address'
  #       }]
  #     }]  
  getAddressColumns : (columns)->    
    @options.columns = @options.columns || []
    @options.columns.filter ( column )=>
      column.required_attribute && column.required_attribute == 'address'



  # @Description: checks if any of the current top level columns has nested options
  # @return has_nested_status:boolean
  hasNestedColumns : (columns)->
    @options.columns = @options.columns || []    
    nested_cols = @options.columns.filter ( column )=>
      column.options
    nested_cols && nested_cols.length > 0


  # @Description: gets all the current top level columns with nested options
  # @return cols_with_nested_options:array 
  #   E.g.
  #     [{ 
  #       col_name : 'detailed_page_href'
  #       dom_query : '.listing_product_name'
  #       required_attribute : 'href'
  #       options:
  #         columns : [{
  #          col_name : 'description'
  #           dom_query : '.tabfield18504'
  #     }, { 
  #       col_name : 'detailed_page_href2'
  #       dom_query : '.listing_product_name'
  #       required_attribute : 'href'
  #       options:
  #         columns : [{
  #          col_name : 'description2'
  #           dom_query : '.tabfield18504'
  #       }]
  #     }]  
  getColumnsWithNest : (columns)->
    @options.columns = @options.columns || []    
    @options.columns.filter ( column )=>
      column.options



  # @Description: merges the scraped results with the current results
  # @param: old_results:object  
  # @param: new_results:object
  # @return: merged_results:object
  merge_results : (old_results, new_results)->
    merged_results = {}
    
    old_results && Object.keys(old_results).forEach (attrname)->
      merged_results[attrname] = old_results[attrname]

    new_results && Object.keys(new_results).forEach (attrname)->
      merged_results[attrname] = new_results[attrname]

    merged_results



  # @Description: makes an exact replica of the object and returns the copy
  # @param: object_to_clone:object
  # @return: cloned_obj:object
  clone : (object_to_clone)->
    cloned_obj = kson.parse(kson.stringify(cloned_obj))



module.exports = ListingPageScrapper

# @Description: this procedure is ran if script is called directly. Used mainly for unit testing purposes.
if !module.parent

  global.CONFIG = null;
  global.ENV = (process.env['NODE_ENV'] || 'development').toLowerCase()
  try 
    CONFIG = kson.parse(fs.readFileSync('../config/config.js').toString())[ENV];
  catch error
    console.log('cannot parse config.js')
    process.exit(1)

  regex_test = {
    "origin_url": "http://www.artslant.com/la/venues/list",
    "columns": [
      {
          "col_name": "phone number extracted using regex",
          "xpath": "//*[@id='thelist']/tr/td/table/tbody/tr/td[3]",
          "required_attribute": "textContent",
          "regex_pattern": '^\d{3}(-|.)\d{3}(-|.)\d{4}$',
          "regex_flag": "ig",
          "regex_group": 0
      }
    ],
    "next_page": {
        "dom_query": "a.next"
    }
  }    
  
  jquery_test = {
    "origin_url": "http://www.mdscollections.com/cat_mds_accessories.cfm",
    "columns": [
      {
          "col_name": "innerText",
          "dom_query": "div",
          "required_attribute": "innerText"

      },
      {
          "col_name": "textContent",
          "dom_query": "div",
          "required_attribute": "textContent"

      },
      {
          "col_name": "innerHTML",
          "dom_query": "div",
          "required_attribute": "innerHTML"

      }            
    ],
    "next_page": {
        "dom_query": "a.next"
    }    
  }

  xpath_test = {
    "origin_url": "http://www.mdscollections.com/cat_mds_accessories.cfm",
    "columns": [
      {
          "col_name": "innerText",
          "xpath": "//div",
          "required_attribute": "innerText"

      },
      {
          "col_name": "textContent",
          "xpath": "//div",
          "required_attribute": "textContent"

      },
      {
          "col_name": "innerHTML",
          "xpath": "//div",
          "required_attribute": "innerHTML"

      }            
    ],
    "next_page": {
        "dom_query": "a.next"
    }    
  }

  wb = {
      "origin_url": "http://www.wbshop.com/",
      "exclude_jquery": true,
      "columns": [
          {
              "col_name": "main category",
              "dom_query": "li[id*='catNav']>a"
          },
          {
              "col_name": "main category page",
              "dom_query": "li[id*='catNav']>a",
              "required_attribute": "href"
          }
      ]
  }

  fun = {
      "origin_url": "https://angel.co/startups",
      "columns": [
          {
              "col_name": "startup",
              "xpath": "/html[1]/body[1]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div/div[1]/div[2]/div[1]/a[1]",
              "required_attribute": "href"
          }
      ],
      "next_page": {
          "xpath": "/html[1]/body[1]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[26]/div[1]"
      }
  }
  
  facebook = {
    origin_url : "https://www.facebook.com/garyjob",
    columns : [{
      col_name : 'user name',
      dom_query : '.actorDescription.actorName a'
    }],
    cookies :  [{
        "domain": ".facebook.com",
        "expirationDate": 1429285283,
        "hostOnly": false,
        "httpOnly": true,
        "name": "datr",
        "path": "/",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "eJaFULnyfz49amZN65veYEIs"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1429285284,
        "hostOnly": false,
        "httpOnly": true,
        "name": "lu",
        "path": "/",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "ghzeRf02GWAoez3wpo2j0i4g"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1375108434,
        "hostOnly": false,
        "httpOnly": false,
        "name": "c_user",
        "path": "/",
        "secure": true,
        "session": false,
        "storeId": "0",
        "value": "590788071"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1375108434,
        "hostOnly": false,
        "httpOnly": false,
        "name": "csm",
        "path": "/",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "2"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1375108434,
        "hostOnly": false,
        "httpOnly": true,
        "name": "fr",
        "path": "/",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "03XZbUvNdKvYZidQI.AWUGAdH-2l2GamX4gfQFuu__ZMI.BQhd9d.EG.AAA.AWXZ9jCc"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1375108434,
        "hostOnly": false,
        "httpOnly": true,
        "name": "s",
        "path": "/",
        "secure": true,
        "session": false,
        "storeId": "0",
        "value": "Aa7kxTMRgZ57Rf4a.BRbsKk"
    },
    {
        "domain": ".facebook.com",
        "expirationDate": 1375108434,
        "hostOnly": false,
        "httpOnly": true,
        "name": "xs",
        "path": "/",
        "secure": true,
        "session": false,
        "storeId": "0",
        "value": "1%3AiSYIYenAUcWBNQ%3A2%3A1366213284"
    },
    {
        "domain": ".facebook.com",
        "hostOnly": false,
        "httpOnly": false,
        "name": "sub",
        "path": "/",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "1610621440"
    },
    {
        "domain": ".facebook.com",
        "hostOnly": false,
        "httpOnly": false,
        "name": "p",
        "path": "/",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "189"
    },
    {
        "domain": ".facebook.com",
        "hostOnly": false,
        "httpOnly": false,
        "name": "act",
        "path": "/",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "1372517643385%2F9"
    },
    {
        "domain": ".facebook.com",
        "hostOnly": false,
        "httpOnly": false,
        "name": "presence",
        "path": "/",
        "secure": true,
        "session": true,
        "storeId": "0",
        "value": "EM372517710EuserFA2590788071A2EstateFDsb2F1372516548181Et2F_5b_5dElm2FnullEuct2F1372516548181EtrFA2close_5fviewA2EtwF1006088645EatF1372517645129EwmlFDfolderFA2inboxA2Ethread_5fidFA2user_3a227700876A2CG372517710122CEchFDp_5f590788071F39CC"
    },
    {
        "domain": ".facebook.com",
        "hostOnly": false,
        "httpOnly": false,
        "name": "wd",
        "path": "/",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "1345x647"
    }]
  }

  kson_regexp = 
    {
        "origin_url": "http://scottlocklin.wordpress.com/",
        "columns": [
            { "col_name": "tags"
            , "xpath": "//*[@class='meta']"
            , "regex_pattern": /Posted in (.*) by (?:.*) on (.*)/
            , "regex_group": 1
            }
        ]
    }
  
  adam_regexp = 
    {
        "origin_url": "http://www.artslant.com/la/venues/list",
        "columns": [
            {
                "col_name": "website url",
                "xpath": "//*[@id='thelist']/tr/td/table/tbody/tr/td[3]/a[1]",
                "required_attribute": "href"
            },
            {
                "col_name": "email address extracted using regex",
                "xpath": "//*[@id='thelist']/tr/td/table/tbody/tr/td[3]/a[2]",
                "required_attribute": "href",
                "regex_pattern": /[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+.[a-zA-Z]{2,4}/ig,
                "regex_group": 0
            }
        ],
        "next_page": {
            "dom_query": "a:contains('next')"
        }
    }  
  
  ian_regexp = 
    {
        "origin_url": "http://www.musiceducation.asia/events/",
        "columns": [
            {
                "col_name": "Name of event",
                "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[3]/div[1]/div[2]/ul[1]/li[1]/ul[1]/li/div[1]/a[1]"
            },
            {
                "col_name": "Event URL",
                "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[3]/div[1]/div[2]/ul[1]/li[1]/ul[1]/li/div[1]/a[1]",
                "required_attribute": "href"
            },
            {
                "col_name": "Event description",
                "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[3]/div[1]/div[2]/ul[1]/li[1]/ul[1]/li/div[2]"
            },
            {
                "col_name": "Event Location",
                "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[3]/div[1]/div[2]/ul[1]/li[1]/ul[1]/li/div[2]",
                "regex_pattern": /[^.]+/gi,
                "regex_group": 2
            }
        ]
    }  
 
  chrome_exp = {
      "origin_url": "http://events.insing.com/search/?q=events",
      "columns": [
          {
              "col_name": "event title",
              "xpath": "/html[1]/body[1]/div/section[1]/div[1]/div[1]/div[2]/section[1]/div[2]/ul[1]/li/div[1]/h4[1]/a[1]"
          }
      ],
      "next_page": {
          "xpath": "/html[1]/body[1]/div/section[1]/div[1]/div[1]/div[2]/section[1]/div[2]/div[1]/ul[1]/li[3]/a[1]"
      }
  }  
  
  manta = {
      "origin_url": "http://www.manta.com/c/mxj1y08/clarion-inn-bourbon-bistro-and-bar",
      "columns": [
          {
              "col_name": "page",
              "dom_query": "body"
          }
      ],
      wait : 10000
      "cookies": [
          {
              "domain": "www.manta.com",
              "hostOnly": true,
              "httpOnly": false,
              "name": "OX_plg",
              "path": "/mb_35_B315F000_000",
              "secure": false,
              "session": true,
              "storeId": "0",
              "value": "swf|sl|qt|shk|pm"
          },
          {
              "domain": "www.manta.com",
              "hostOnly": true,
              "httpOnly": false,
              "name": "abtest_v",
              "path": "/",
              "secure": false,
              "session": true,
              "storeId": "0",
              "value": "quick_claim&quick_claim&static_page&ppc_landing_ads.mantahelps&profile_stats&show_me_how&version&104&site_wide&member_service.ms3&leadgen&leadgen.v1&upsell_test&upsell_control&ppc_login&ppc_login.ppc2&adsense&b&afs_split_test&afs_split_test.treatmentc&upsellbutton&upsellbutton.b&mobile_adsense&d&suggested_company_follow_module_split_test&suggested_company_follow_module_split_test.treatmentb&manta_local_survey&manta_local_survey.manta_local_survey-off"
          },
          {
              "domain": "www.manta.com",
              "expirationDate": 1441349205,
              "hostOnly": true,
              "httpOnly": false,
              "name": "__atuvc",
              "path": "/",
              "secure": false,
              "session": false,
              "storeId": "0",
              "value": "2%7C36"
          },
          {
              "domain": ".www.manta.com",
              "expirationDate": 1558277207,
              "hostOnly": false,
              "httpOnly": false,
              "name": "__ar_v4",
              "path": "/",
              "secure": false,
              "session": false,
              "storeId": "0",
              "value": "XP33ORT55VG75JO4QESBQK%3A20130902%3A10%7CCOMGYRXIRRAWLM6VSYKRKM%3A20130902%3A16%7CW7FRNIMMOJFHPJ3V6NB3HM%3A20130902%3A16%7CP2UUTSPV2RD27JFEH3QWTE%3A20130902%3A6"
          }
      ],
      "client_version": "1.2.4"
  }  
  
  geo_test = {
      "exclude_jquery" : true,
      "origin_url": "http://maps.googleapis.com/maps/api/geocode/xml?sensor=false&address=29%20Club%20Street+Singapore",
      "columns": [
          {
              "col_name": "Latitude",
              "xpath": "//GeocodeResponse/result/geometry/location/lat",
              required_attribute : 'textContent'
          },
          {
              "col_name": "Longitude",
              "xpath": "//GeocodeResponse/result/geometry/location/lng",
              required_attribute : 'textContent'              
          },
          {
              "col_name": "Postal Code",
              "xpath": "//GeocodeResponse/result/address_component[5]/long_name",
              required_attribute : 'textContent'              
          }
      ]
  }

  cookie_test = {
    "origin_url": "http://localhost:9909/",
    "columns": [
      {
          "col_name": "body",
          "dom_query" : "body"
      },
      {
          "col_name": "name",
          "xpath": "/html[1]/body[1]/div/div[1]/div[2]/div[1]/div[5]/div[3]/ol[1]/li[1]/div[1]/h3[1]/a[1]"
      }
    ],
    "cookies": [
        {
            "domain": "localhost",
            "hostOnly": true,
            "httpOnly": false,
            "name": "X-LI-IDC",
            "path": "/cookie",
            "secure": false,
            "session": true,
            "storeId": "0",
            "value": "C1"
        }
    ]
  }

  settings = {}
  settings.config = CONFIG
  settings.task_info_obj = cookie_test
  ls = new ListingPageScrapper settings, ()->
    ls.processTask (scenario, interim_results)->
        # When data is harvested by this Krake
        console.log '[INTERIM SCRAPPING] %s', scenario
        console.log interim_results
        
      , ()->
        # when all tasks on this page are finished
        
        console.log '[FINAL SCRAPPING]'

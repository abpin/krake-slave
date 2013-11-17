# @Description: this class extends the scrapper object and is specialized in extracting physical address 
#   from a particular field and translating it into an geolocation
#   In this current version, it only extracts Country name and Zip Code
#   Heuristics:
#     Detect for country_name in chunk of text
#     Assume chunk of number before or after that country_name to be the zipcode
#     Detect for the terms 
#       road
#       street

# Dependencies
fs = require 'fs'
async = require 'async'
geocoder = require 'geocoder'
natural = require 'natural'
tokenizer = new natural.WordTokenizer()

class GeolocationScrapper

  # @Description: default constructor
  # @param: options:object
  #   E.g. task_option_obj
  #     options = 
  #       columns : [{
  #           col_name : 'office_location'
  #           dom_query: '.today-deal-partner',
  #           required_attribute: 'address',
  #           latitude: 'office_lat',
  #           longitude: 'office_lng'
  #         },{
  #           col_name : 'hq_location'
  #           dom_query: '.hq-location',
  #           required_attribute: 'address',
  #           latitude: 'hq_lat',
  #           longitude: 'hq_lng'
  #       }]
  #       data :
  #         source_name : 'jobstreet.com'
  #         title : 'something'
  #         hq_location : '2157 Geylang'
  #         office_location : '2157 Sengkang'
  # @param: initial_callback:function()
  constructor: (options)->
    @countries = fs.readFileSync(__dirname + '/../natural_language_patterns/countries').toString().split(/\n/)  
    @minimal_match_threshold = 0.9
    Scrapper.call this, options, ()->
      initial_callback()
  
  # @Description: The default interface external processes use to interact with a Scrapper object.
  # @param: interim_callback:function(scenario:string, updated_data_objs:array)
  #   Current scenarios:
  #     success
  #     failed
  #   E.g. updated_data_objs
  #     [{
  #         source_name : 'jobstreet.com'
  #         title : 'something'
  #         hq_location : 'something thing really raw Singapopre 120429, Singapore 133429'
  #         hq_address : 'Singapopre 120429'
  #         hq_lat : 1.3136053
  #         hq_lng: 103.7634956
  #         office_location : 'something thing really raw Singapopre 120429, Singapore 133429'
  #         office_address : 'Singapore 133429'
  #         office_lat: 1.3136053
  #         office_lat: 103.7634956
  #       },{
  #         source_name : 'jobstreet.com'
  #         title : 'something'
  #         hq_location : 'something thing really raw Singapopre 120429, Singapore 133429'
  #         hq_address : 'Singapopre 120429'
  #         hq_lat : 1.3136053
  #         hq_lng: 103.7634956
  #         office_location : 'something thing really raw Singapopre 120429, Singapore 133429'
  #         office_address : 'Singapore 133429'
  #         office_lat: 1.3136053
  #         office_lat: 103.7634956  
  #     }]
  #     
  # @param: final_callback:function()  
  processTask : (interim_callback, final_callback)->
    console.log 'gs: processing task'
    extracted_entries = []
    
    
    # @Description: makes an exact replica of the object and returns the copy
    # @param: object_to_clone:object
    # @return: cloned_obj:object
    clone = (object_to_clone)->
      keys = Object.keys object_to_clone
      cloned_obj = {}
      for x in [0...keys.length]
        cloned_obj[keys[x]] = object_to_clone[keys[x]]

      cloned_obj
    
    
    
    async.forEachSeries @options.columns, (curr_col, next)=>
        
        # extracts country name and zipcode
        address_text = @prior_data_obj[curr_col.col_name]
                    
        # extracts geolocation for each country name and zipcode extracted
        @fetchGeolocation address_text, (geolocations)=>
           
          for x in [0...geolocations.length]
            updated_data_obj = clone(@prior_data_obj) 
            extracted_entries.push(updated_data_obj)
                        
            lat_key = curr_col.latitude || curr_col.col_name + '_lat'
            lng_key = curr_col.longitude || curr_col.col_name + '_lng'
            country_key = curr_col.country || curr_col.col_name + '_country'
            zip_key = curr_col.zipcode || curr_col.col_name + '_zip'
            updated_data_obj[lat_key] = geolocations[x].lat
            updated_data_obj[lng_key] = geolocations[x].lng
            # updated_data_obj[country_key] = curr_add_obj.country
            updated_data_obj[country_key] = "NA"
            # updated_data_obj[zip_key] = "NA"
            updated_data_obj[zip_key] = "NA"
            
          next()
              
      , (err)=>
        interim_callback && interim_callback 'listing with geolocation information attached', extracted_entries
        final_callback && final_callback()



  # @Description: Extract physical address
  # @param raw_address_text:string
  # @return array_of_country_n_zipcodes:array
  #   E.g.
  #     ['Singapore 120429', 'Singapore 429653']
  extractCountryZipcodeFromText : (raw_address_text)->
    raw_tokens = tokenizer.tokenize(raw_address_text)
    array_of_country_n_zipcodes = []
    for x in [0...raw_tokens.length]
      if @isCountry raw_tokens[x]
        curr_entry = {}
        curr_entry.country = raw_tokens[x]
        # Checks the token before this token for zipcode
        @isZipCode(raw_tokens[x - 1]) && (curr_entry.zip = raw_tokens[x - 1]) && (array_of_country_n_zipcodes.push(curr_entry))

        # Checks the token before this token for zipcode          
        @isZipCode(raw_tokens[x + 1]) && (curr_entry.zip = raw_tokens[x + 1])  && (array_of_country_n_zipcodes.push(curr_entry))

    array_of_country_n_zipcodes



  # @Description: Checks if the given token matches a country name
  # @param raw_token:string
  # @return is_country:boolean
  isCountry : (raw_token)->
    for x in [0...@countries.length]
      percentage_match = natural.JaroWinklerDistance raw_token,@countries[x]
      if percentage_match > @minimal_match_threshold
        return true

    return false



  # @Description: Checks if the given token matches a zipcode
  #   TODO: implement full set of zipcode regex using the list at URL below
  #   http://en.wikipedia.org/wiki/List_of_postal_codes
  # @param raw_token:string
  # @return is_zipcode:boolean
  isZipCode : (raw_token)->
  
    # Singapore zipcode pattern
    if !isNaN(raw_token) && isFinite(raw_token) && raw_token.length == 6
      true
    else
      false
  
  
  
  # @Description: Given a string with valid physcial address, it returns an array of matching geolocation tags
  # @param processed_address_text:string
  # @param callback:function(consolidated_geocoordiates:array)
  #   E.g.
  #   [{
  #       lat: 1.3136053
  #       lng: 103.7634956
  #     },{
  #       lat: 1.215
  #       lng: 103.333
  #   }]
  fetchGeolocation : (processed_address_text, callback)->
    geocoder.geocode processed_address_text, ( err, data )=>
      consolidated_geocoordiates = []
      for x in [0...data.results.length]
        consolidated_geocoordiates.push data.results[x].geometry.location

      callback consolidated_geocoordiates



module.exports = GeolocationScrapper

# @Description: this procedure is ran if script is called directly. Used mainly for unit testing purposes.
if !module.parent

  options = 
    columns : [{
        col_name : 'office_location'
        dom_query: '.some-class'
        required_attribute: 'address'
        country: 'office_country'
        zipcode: 'office_zipcode'
        latitude: 'office_lat'
        longitude: 'office_lng'   
      },{
        col_name : 'hq_location'
        dom_query: '.some-class'
        required_attribute: 'address'     
      }]
    data :
      source_name : 'jobstreet.com'
      title : 'something'
      hq_location : 'Partner\n      120429 Singapore   Old Hong Kong Tea House\n \n86 East Coast Road Block A\n\tKatong Village (Next to Katong Mall)\n\tSingapore 428788\n \nTel: 63451932\n \nBusiness Hours: 24 Hours\n \nWebsite: www.oldhongkong.com.sg'
      office_location : 'Partner\n        Kemi Wellness\n \n92 Tanjong Pagar Road\n\tSingapore 088513\n \nTel: 63237117\n \nOperating Hours: 11.30am to 8.30pm (Mon – Fri); 10.30am to 7pm (Sat); closed on Sundays, eve of PH & PH\n \nEmail: enquiry@kemi.com.sg\n\tWebsite: www.kemi.com.sg'

  ggs = new GeolocationScrapper options, ()->
  ggs.processTask (scenario, interim_results)->
      console.log '[INTERIM SCRAPPING] %s', scenario
      console.log interim_results
    , ()->
      console.log '[FINAL SCRAPPING]'
    
# @Description: Listens to Redis Queue and process tasks. 
#   Returns results if no more tasks to perform
#   Creates and adds a new task to queue for each sub task
kson = require 'kson'
fs = require 'fs'
QueueInterface = require "krake-queue"
ListingPageScrapper = require './scrappers/listing_page_scrapper'
GeolocationScrapper = require './scrappers/geolocation_scrapper'
Phoenix = require("krake-toolkit").usage.phoenix
ProcessTimer = require './helpers/process_timer.coffee'
DeclarativeVariableHelper = require("krake-toolkit").query.declarative_var
exec = require('child_process').exec
request = require 'request'

class NetworkSlave

  # @Description: Default constructor
  # @param: 
  #   - config:object
  #     - canRotateIP:string
  #     - phantomServer:object
  #     - redis:object
  #       - host:string
  #       - port:string
  #       - queueName:string
  #       - scrapeMode:string  
  #           depth | breadth
  #             depth = go as deep as possible before going beadth
  #           breadth
  #             breadth = go as broad as possiblebefore going deep  
  # @param: initialCallBack:function()
  constructor: (@config, initialCallBack)->
    @publishStack = []
    @queueName = @config.redis.queueName
    @dvh = new DeclarativeVariableHelper()
    @currentState = false
    @redisInfo = @config.redis
    @qi = new QueueInterface @redisInfo
    @setEventListeners()
    @currentState = 'idle'
    @tryGetTask(@queueName)
    @outputCallback = false
    @processPublishStack()
        
    if @config.canRotateIP == "can"
      console.log '[NETWORK_SLAVE] : Reincarnation mode'    
      @canRotateIp = true
      @pageCrawlsLeft = @config.phoenixServer.pageCrawlLimit
      
    else 
      console.log '[NETWORK_SLAVE] : Resurrection mode'        
      
    
  
  # @Description: set events to listen for
  setEventListeners: ()->
  
    # Listens for status ping - if not idling then beg for mercy
    @qi.setEventListener 'status ping', (queueName, resObj)=>
      if @currTaskQueueName != queueName then return
      if @currentState =='busy' || @publishStack.length > 0
        console.log '[NETWORK_SLAVE] : status ping received. ' + 
          '\n\t\tRequest for delay in termination'
        @qi.broadcast @currTaskQueueName, 'mercy'
  
  
    # Listens for abort task event - if processing any task, aborts it
    @qi.setEventListener 'kill task', (queueName, resObj)=>
      console.log "[NETWORK_SLAVE] : Termination challenger : %s versus %s", queueName, @currTaskQueueName
      if @currTaskQueueName != queueName then return
      console.log '[NETWORK_SLAVE] : Order to abort task in entire slave cluster' +
        '\n\t\tEntering into Kamikaze mode'
      @is_kamikaze = true
      @qi.stop_send = true
      @kamikaze()

  
  
    # Listens for new task
    @qi.setEventListener 'new task', (queue_name)=>
      # @log '[NETWORK_SLAVE] : new task announcement heard'
      switch @currentState
        when 'idle'
          @tryGetTask(queue_name)
          
        when 'queueing', 'busy', 'killed'
          @log '[NETWORK_SLAVE] : Not responding to new task. ' + @currentState



  # @Description: gets the next task on the tray
  tryGetTask: (queue_name)->
    @currentState = 'queueing'  
    @log '[NETWORK_SLAVE] : trying to get task from queue'  

    @qi.getTaskFromQueue queue_name, (task_info_obj)=>
      @task_info_obj = task_info_obj
      if task_info_obj
        @currentState = 'busy'
        @currTaskQueueName = queue_name        
        @log '[NETWORK_SLAVE] : Assigned to work on page ' + 
          '\n\t\tURL : ' + task_info_obj.origin_url, task_info_obj, 'information'
        
        # job is not restricted by Quota
        if !task_info_obj.quota_limited
          @doTask task_info_obj
          
        else
          @takeToken task_info_obj, (do_task)=>
            if do_task
              @doTask task_info_obj
              
            else
              # when out of quota wait for cluster to refresh
              @currentState = 'killed'
              @log '[NETWORK_SLAVE] : Current job ran out of quota.' + 
                '\n\t\tJob aborted.', task_info_obj, 'warning'
              @tryGetTask(queue_name)
        
      else
        @currentState = 'idle'      
        @log '[NETWORK_SLAVE] : Could not get any task to do from queue. Back to idling'



  # @Description: tries to consume token from available quota for this job. If no more quota then discards the job
  # @param: task_info_obj:Object
  # @param: callback:function(do_task:boolean)
  takeToken: (task_info_obj, callback)->
    if task_info_obj && task_info_obj.quota_limited && task_info_obj.pgParams && 
    task_info_obj.pgParams.tableName && task_info_obj.origin_url
    
      @log '[NETWORK_SLAVE] : consuming token ' + task_info_obj.pgParams.tableName +
      '\n\t\tURL : ' + task_info_obj.origin_url
      , 'information'
          
      url = @config.usageServer + '/take-token/' + task_info_obj.auth_token + '/' + 
            task_info_obj.pgParams.tableName + '/' + task_info_obj.origin_url
            
      request url, (error, response, do_task)=>
        if !error && response.statusCode == 200 && do_task == 'true'
            @log '[NETWORK_SLAVE] : Token consumned ' + 
              '\n\t\tTableName : ' + task_info_obj.pgParams.tableName + 
              '\n\t\tURL : ' + task_info_obj.origin_url, task_info_obj, 'information'
            callback true
            
        else
            @log '[NETWORK_SLAVE] : Could not get token  ' + 
              '\n\t\tTableName' + task_info_obj.pgParams.tableName + 
              '\n\t\tURL : ' + task_info_obj.origin_url, task_info_obj, 'error'
            callback false


  
  # @Description: tries to consume available quota if there is still quota for this job
  # @param: task_info_obj:Object
  # @param: callback:function(do_job:boolean)
  returnToken: (task_info_obj, callback)->
      
    if task_info_obj && task_info_obj.quota_limited && task_info_obj.pgParams && task_info_obj.pgParams.tableName && task_info_obj.origin_url
      @log '[NETWORK_SLAVE] : Page processing failed, returning token' +
        '\n\t\tURL : ' + task_info_obj.origin_url +
        '\n\t\tTableName' + task_info_obj.pgParams.tableName, task_info_obj, 'error'
      
      url = @config.usageServer + '/return-token/' + task_info_obj.auth_token + '/' + task_info_obj.pgParams.tableName
      request url, (error, response, body)=>
        @log '[NETWORK_SLAVE] : Token returned ' +
          '\n\t\tURL : ' + task_info_obj.origin_url +
          '\n\t\tTableName' + task_info_obj.pgParams.tableName, task_info_obj, 'information'
        callback && callback()
        
    else
      @log '[NETWORK_SLAVE] : Page processing failed', task_info_obj, 'information'
      callback && callback()



  # @Description: Handles the various task types that comes in from the queue
  #      Doing a task and is busy shall not listen in for more task
  # @param: task_info_obj:Object
  #   E.g. task_info_obj
  #     options = 
  #       origin_url : 'http://www.mdscollections.com/cat_mds_accessories17.cfm'
  #       columns : [{
  #           col_name : 'title'
  #           dom_query : '.listing_product_name' 
  #         }, { 
  #           col_name : 'price'
  #           dom_query : '.listing_price' 
  #         }, { 
  #           col_name : 'detailed_page_href'
  #           dom_query : '.listing_product_name'
  #           required_attribute : 'href'
  #       }]
  #       next_page :
  #         dom_query : '.listing_next_page'
  #       detailed_page :
  #         columns : [{
  #           col_name : 'description'
  #           dom_query : '.tabfield18504'
  #         }]
  #       task_type: 'listing page scrape' | 'geolocation scrape'
  #       data : 
  #         source_name : 'Chrollusion committers' 
  #
  doTask: (task_info_obj)->
    switch task_info_obj.task_type
      when 'listing page scrape' then @doListingPage task_info_obj
      when 'geolocation scrape' then @doGeolocation task_info_obj    
      else
        @log '[NETWORK_SLAVE] : Do not know how to process task.\n\n'
        @currentState = 'idle'
        @tryGetTask(task_info_obj.task_id)



  doListingPage: (task_info_obj)->

    @log '[NETWORK_SLAVE] : Processing page ' + 
      '\n\t\tURL : ' + task_info_obj.origin_url + 
      '\n\t\tDuration is dependent on actual server speed ', task_info_obj, 'information'
    
    settings = {}
    settings.config = @config
    settings.task_info_obj = task_info_obj
    @ls = new ListingPageScrapper settings
    
    statusUpdate = (time_left, time_waited)=>
      @log '[NETWORK_SLAVE] : Waiting for response to page request ' + 
        '\n\t\tURL : ' + task_info_obj.origin_url +
        '\n\t\tWaited ' + time_waited + ' seconds.' + 
        '\n\t\tWaiting ' + time_left + ' additional seconds.' + 
        '\n\t\tPlease be patient and thank you for waiting.',
        task_info_obj, 'information'
    
    abortTask = ()=>
      @returnToken task_info_obj
      task_info_obj.retries = task_info_obj.retries || 1
      task_info_obj.retries += 1
    
      if task_info_obj.retries < 5 then queuePosition = 'head'
      else queuePosition = 'bad'
      
      @qi.addTaskToQueue task_info_obj.task_id, 'listing page scrape', task_info_obj, queuePosition, ()=>
        @log '[NETWORK_SLAVE] : Page timeout. Page returned to tray ' + 
          '\n\t\tURL : ' + task_info_obj.origin_url +
          '\n\t\tretries : ' + task_info_obj.retries +
          '\n\t\tmethod : %s' + queuePosition
          , task_info_obj, 'information' 
    
      if !@is_kamikaze
        @currentState = 'killed'
        @log '[NETWORK_SLAVE] : Entering into kamikaze mode.'
        @is_kamikaze = true
        @qi.stop_send = true
        @kamikaze()
      
      else if @is_kamikaze
        @log '[NETWORK_SLAVE] : In kamikaze mode. Waiting for death.'
        @currentState = 'killed'    
    
    timeLimit = (@config.phantomServer && @config.phantomServer.timeout) || 120
    timeLimit += parseInt((task_info_obj.wait || 0) / 1000)
    timer = new ProcessTimer timeLimit, 5, statusUpdate, abortTask
    timer.startTimer()
    
    # [ListingPageScrapper] when results are scrapped and returned from the current page
    @ls.processTask (scenario, interim_results)=> 
        # interim callback scenarios
        switch scenario
              
          when 'next page href extracted'
            # declare new task to scrape new listing page
            next_page_url = interim_results
            @log '[NETWORK_SLAVE] : URL for next page in listing : ' + next_page_url, task_info_obj, 'information'
            new_task_info = @duplicateTaskInfoObj(task_info_obj)
            new_task_info.origin_url = next_page_url
            new_task_info.to_click = task_info_obj.to_click
            new_task_info.columns = task_info_obj.columns
            new_task_info.exclude_jquery = task_info_obj.exclude_jquery
            new_task_info.wait = task_info_obj.wait
            new_task_info.data = task_info_obj.data
            task_info_obj.next_page && (new_task_info.next_page = task_info_obj.next_page)
            @qi.addTaskToQueue new_task_info.task_id, 'listing page scrape', new_task_info, 'head', ()->



          when 'listings with need to scrape geolocation information'
      
            @log '[NETWORK_SLAVE] : Extracted ' + interim_results.length + ' record(s) ' + 
              '\n\t\tURL : ' + task_info_obj.origin_url + 
              '\n\t\tNext action : Get address information', task_info_obj, 'information'
        
            # Spawns new threads for mining geolocation
            address_cols = @ls.getAddressColumns()
            interim_results.forEach (current_result)=>
              new_task_info_obj = @duplicateTaskInfoObj(task_info_obj)
              new_task_info_obj.columns = address_cols
              new_task_info_obj.data = current_result
              @qi.addTaskToQueue task_info_obj.task_id, 'geolocation scrape', new_task_info_obj, 'tail', ()->



          when 'listings with need to scrape detailed pages extracted'
      
            @log '[NETWORK_SLAVE] : Extracted ' + interim_results.length + ' record(s)' +
              '\n\t\tURL : ' + task_info_obj.origin_url +
              '\n\t\tNext Action : Dive into sub pages', task_info_obj, 'information'

            # Spawns new threads for mining subpages
            nested_cols = @ls.getColumnsWithNest()
            nested_cols.forEach (selected_nest_col)=>
              interim_results.forEach (current_result)=>

                new_task_info_obj = @duplicateTaskInfoObj(task_info_obj)
                new_task_info_obj.to_click = selected_nest_col.options.to_click                
                new_task_info_obj.columns = selected_nest_col.options.columns
                new_task_info_obj.exclude_jquery = selected_nest_col.options.exclude_jquery
                new_task_info_obj.wait = selected_nest_col.options.wait
                new_task_info_obj.data = current_result
                selected_nest_col.options.next_page && (new_task_info_obj.next_page = selected_nest_col.options.next_page)
                
                # Event whereby there is a nested origin_url object
                if selected_nest_col.options.origin_url
                  new_task_info_obj.origin_url = selected_nest_col.options.origin_url
                  @dvh.convertOriginUrl new_task_info_obj, selected_nest_col.col_name, (new_task_info_obj)=>                          
                    @qi.addTaskToQueue task_info_obj.task_id, 'listing page scrape', new_task_info_obj, 'tail', ()->
              
                # Event whereby the column with nested option has valid URL to link to 
                else if current_result[selected_nest_col['col_name']]                     
                  new_task_info_obj.origin_url = current_result[selected_nest_col['col_name']]
                  @qi.addTaskToQueue task_info_obj.task_id, 'listing page scrape', new_task_info_obj, 'tail', ()->
            
                # Event whereby the column with nested option has no URL to link to 
                else if !current_result[selected_nest_col['col_name']]
                  @qi.broadcast task_info_obj.task_id, 'results', current_result
                  @appendPublishStack task_info_obj, current_result



          when 'listings with no need to scrape detailed pages extracted'
      
            @log '[NETWORK_SLAVE] : Extracted ' + interim_results.length + ' result(s)' + 
              '\n\t\tURL : ' + task_info_obj.origin_url + 
              '\n\t\tNext action : return records. ', task_info_obj, 'information'
              
            interim_results.forEach (current_result)=>
              @qi.broadcast task_info_obj.task_id, 'results', current_result
              @appendPublishStack task_info_obj, current_result
          
          when 'no new data extracted'
            @qi.broadcast task_info_obj.task_id, 'results', task_info_obj.data
            @appendPublishStack task_info_obj, task_info_obj.data
            
      
      # [ListingPageScrapper] Final callback when all tasks to be done on this page is finished              
      , ()=>
      
        timer.stopTimer()
        @log_usage task_info_obj
      
        # IP rotation sequence check            
        if @canRotateIp
          if @is_kamikaze
            @currentState = 'killed'            
            @log '[NETWORK_SLAVE] : List scraping operation completed. Waiting for death.'
        
          else if !@is_kamikaze && @pageCrawlsLeft <= 0
            @currentState = 'killed'
            @log '[NETWORK_SLAVE] : List scraping operation completed. Time to rotate IP address.' +
              'Entering into kamikaze mode.'
            @is_kamikaze = true
            @qi.stop_send = true
            @kamikaze()                
                        
          else if !@is_kamikaze && @pageCrawlsLeft > 0
            @pageCrawlsLeft -= 1
            @log '[NETWORK_SLAVE] : Visit from the angel of death. ' + 
              '\n\t\tPage crawls left :' + @pageCrawlsLeft              
            @currentState = 'idle'
            @log '[NETWORK_SLAVE] : List scraping operation completed.'
            @tryGetTask task_info_obj.task_id
      
        # normal process
        else
          if !@is_kamikaze
            @currentState = 'idle'
            @log '[NETWORK_SLAVE] : List scraping operation completed.'
            @tryGetTask task_info_obj.task_id
      
          else if @is_kamikaze
            @currentState = 'killed'            
            @log '[NETWORK_SLAVE] : List scraping operation completed. Waiting for death.'
      
  

  doGeolocation: (task_info_obj)->

    # TODO: need to handle situation whereby the indicated column for mining does not exist in record
    @log '[NETWORK_SLAVE] : Converting addresses into geolocations', task_info_obj, 'information'
    @gs = new GeolocationScrapper task_info_obj

    @gs.processTask (scenario, geo_results)=>
      results_length = geo_results.length         
      @log '[NETWORK_SLAVE] : Extracted ' + results_length + 
        ' results with their addresses ', task_info_obj, 'information'
      if geo_results.length == 0
        results_channel_key = @qi.getResultsChannelKey task_info_obj.master_id, task_info_obj.task_id
        @qi.broadcast task_info_obj.task_id, 'results', task_info_obj.data        
        @appendPublishStack task_info_obj, task_info_obj.data

      else
        for x in [0...geo_results.length]      
          @log '[NETWORK_SLAVE] : Returning ' + geo_results.length + 
            ' results and their geolocation', task_info_obj, 'information'
          results_channel_key = @qi.getResultsChannelKey task_info_obj.master_id, task_info_obj.task_id
          @qi.broadcast task_info_obj.task_id, 'results', task_info_obj.data          
          @appendPublishStack task_info_obj, geo_results[x]
  
      if !@is_kamikaze
        @currentState = 'idle'
        @log '[NETWORK_SLAVE] : Finished geolocation scrape.'
        @tryGetTask task_info_obj.task_id
    
      else if @is_kamikaze
        @currentState = 'killed'            
        @log '[NETWORK_SLAVE] : Finished geolocation scrape. Waiting for death.'
      
        

  # @Description: sends the interim results object to the publishing server for storage
  appendPublishStack: (task_info_obj, interim_result_obj)->
  
    if task_info_obj && (
        task_info_obj.mongoParams ||
        task_info_obj.rdbParams ||
        task_info_obj.pgParams ||
        task_info_obj.destination_url
      ) && @config && @config.publishingServer
        @publishStack.push [task_info_obj, interim_result_obj]
  
  
  
  # @Description: processes the task in the publish stack
  #   if nothing is happening idle for 5 seconds and then check the stack again — recursive
  processPublishStack: ()->
    if !@config || !@config.publishingServer then return
    
    if @publishStack.length == 0 
      setTimeout ()=>
        @processPublishStack()
      , 5000
      
    else
      result = @publishStack.pop()
      @print_request = true
      options = 
        method: 'POST'  
        url : @config.publishingServer + '/publish'
        json : 
          task_info_obj : result[0]
          interim_result_obj : result[1]

      request options, (error, response, body)=>
        @print_request = false      
        if (!error && response.statusCode == 200)
          console.log '[NETWORK SLAVE] : record was succesfully sent to publishing server'
          @processPublishStack()
          
        else 
          console.log '[NETWORK SLAVE] : Bad record was sent to the server, %s', error
          # @publishStack.push(result)
          @processPublishStack()
        
  
        
  # @Description: logs the usage level of a particular Krake
  #   this particular use case only happens when the scheduler pushes the krakes to our engine
  log_usage: (task_info_obj)->
    if task_info_obj && task_info_obj.quota_limited && task_info_obj.pgParams && task_info_obj.pgParams.tableName && task_info_obj.origin_url
      url = @config.usageServer + '/record-usage/' + task_info_obj.auth_token + '/'+ task_info_obj.pgParams.tableName + '/' + task_info_obj.origin_url
      console.log '[NETWORK_SLAVE] : Recording the usage to %s', url
      request url, (error, response, body)=>
        if !error && response.statusCode == 200
          console.log '[NETWORK_SLAVE] : Usage catpured for %s', url          



  # @Description : copy a standard set of attributes between the current krake and the new krake
  # @return new_task_info_obj:object
  duplicateTaskInfoObj : (task_info_obj)->
    new_task_info_obj = {}
    new_task_info_obj.master_id = task_info_obj.master_id
    new_task_info_obj.task_id = task_info_obj.task_id
    new_task_info_obj.cookies = task_info_obj.cookies
    new_task_info_obj.auth_token = task_info_obj.auth_token                          
    new_task_info_obj.quota_limited = task_info_obj.quota_limited
    new_task_info_obj.rdbParams = task_info_obj.rdbParams
    new_task_info_obj.pgParams = task_info_obj.pgParams
    new_task_info_obj.mongoParams = task_info_obj.mongoParams
    new_task_info_obj.destination_url = task_info_obj.destination_url     
    new_task_info_obj.rawSchema = task_info_obj.rawSchema
    new_task_info_obj



  # @Description: send message output to log
  # @param: message:string
  log: (message, task_info_obj, type)->

    # if there is a master waiting on the other side, return the output message
    if task_info_obj
      logs_obj = {}
      logs_obj.type = type || 'information'
      logs_obj.message = message
      @qi.broadcast task_info_obj.task_id, 'logs', logs_obj

    console.log message


  # @Description : goes into a vicious cycle and dies when is finally idle||Killed and print queue is empty
  kamikaze: ()->

    if @publishStack.length == 0 && !@print_request
      consultHades = ()=>
        if @canRotateIp
          console.log '[NETWORK_SLAVE] : Getting reincarnated'     
          p = new Phoenix @task_info_obj.auth_token, @config.phoenixServer.url
          p.reincarnate()

        else if !@canRotateIp
          console.log '[NETWORK_SLAVE] : Committing Sepuku'
          process.exit(1)

      switch @currentState
        when 'idle', 'queueing', 'killed'
          consultHades()

        when 'busy'
          @task_info_obj && @returnToken @task_info_obj, ()=>
            consultHades()

          !@task_info_obj && consultHades()

    else
      setTimeout ()=>
        console.log '[NETWORK_SLAVE] : Delaying Sepuku by 1 second' + 
          '\n\t\tCurrent state : %s' + 
          '\n\t\tPublishing stack length : %s ' + 
          '\n\t\tPublishing request : %s ', @currentState, @publishStack.length, @print_request
        @kamikaze()
      , 1000    

module.exports = NetworkSlave
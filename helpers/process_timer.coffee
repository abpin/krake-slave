class ProcessTimer
  constructor : (@timeLimit, @timeInterval , @interim_callback, @final_callback)->
    @time_left = @timeLimit
    @time_waited = 0
    
  startTimer : ()->
    @be_patient = setInterval ()=>
      @sendSignal()
    , 5000

    # When time runs out
    @page_timer = setTimeout ()=>
      console.log 'PhantomJs: Page took too long to load. Killed off PhantomJs process'
      @sendAlarm()
    , @timeLimit * 1000
  
  stopTimer : ()->
    @time_left = @timeLimit
    @time_waited = 0
    clearInterval(@be_patient)  
    clearTimeout(@page_timer)    

  sendSignal : ()->
    @time_left -= 5
    @time_waited += 5
    @interim_callback && @interim_callback @time_left, @time_waited
  
  sendAlarm : ()->
    @final_callback && @final_callback()
    
module.exports = ProcessTimer
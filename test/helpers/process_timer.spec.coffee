ProcessTimer = require '../../helpers/process_timer.coffee'

describe "ensure the process timer is working properly ", ()->
  it "should make the final_callback", (done)->
    pt = new ProcessTimer 5, 1, false, false
    done()
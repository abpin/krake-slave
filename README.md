# krake-slave

Starts a slave process that listens to the central queue for scrape jobs to process

## To setup
```console
npm install krake-slave
```

## To run
```console
require 'krake-slave'

CONFIG = {
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

qs = new NetworkSlave CONFIG

```
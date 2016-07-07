# DigitalHomesteadCSVLogger
Digital Homestead CSV Logger creates CSV files of the Walk Over Weigher Data.

To start:
    python2 digitalhomestead/csvlogger.py --config config/config.json
    
Where the `config/config.json` is:
```javascript
    {
      "log_file": "csvlogger.log",
      "log_format": "%(asctime)-15s %(levelname)-7s %(name)s %(filename)s:%(funcName)s:%(lineno)d - %(message)s",
      "csv_output_dir":"data",
      "csv_format": "%Y-%m-%d",
      "accept":{
        "radio_ids":  [111111, 111112, 111113],
        "receiver": [111],
        "location": ["town_name"]
      },
      "pubnub":{
        "subscribe_key":"demo",
        "publish_key":"demo",
        "channels":["channel1"]
      }
    }
```
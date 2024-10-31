#!/bin/sh
curl --location '192.168.1.204:18080/cancel_interest_topic' \
--header 'Content-Type: application/json' \
--data '{
    "interest":[
        {
            "host_name": "raspberrypi-115",
            "interest_topic": [
                {
                    "API":{
                    "address": "http://192.168.1.215:60757/api/status",
                    "cycle": "1"
                    }
                }
            ]
        }
    ]
}'

#!/bin/sh
curl --location '192.168.1.204:18080/add_interest_topic' \
--header 'Content-Type: application/json' \
--data '{
    "interest":[
        {
            "host_name": "plc-244",
            "interest_topic": [
                {
                    "API":{
                    "address": "http://192.168.1.244:5000/api/status",
                    "cycle": "10"
                    }
                }
            ]
        }
    ]
}'

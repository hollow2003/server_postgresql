#!/bin/sh
curl --location '192.168.1.204:18080/get_api' \
--header 'Content-Type: application/json' \
--data '{
    "hosts":["raspberrypi-115"]
}'

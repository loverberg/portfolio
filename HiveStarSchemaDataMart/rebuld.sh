#!/bin/bash
## rebulding DataMart
hive -e "ALTER MATERIALIZED VIEW yellow_taxi.datamart REBUILD;"
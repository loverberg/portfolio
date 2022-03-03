#!/bin/bash
## creating DataMart

hive -v -f dicts.hql;
hive -v -f fact_table.hql;
hive -v -f put_in_fact_table.hql;
hive -v -f view.hql;
hive -e "exit;"


#!/bin/bash

./server/bin/pg_ctl -D data -o "-F -p 5477"  start
if [[ $? -ne 0 ]] ; then
  echo "Error starting porstgreSQL!"
	exit 1
fi

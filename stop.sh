#!/bin/bash

./server/bin/pg_ctl -D data -o "-F -p 5477"  stop

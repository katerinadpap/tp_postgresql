# #!/bin/bash

PREFIX=`pwd`

cd source

make clean 
./configure --prefix=$PREFIX/server
make && make install
if [[ $? -ne 0 ]] ; then
	echo "Compilation error!"
    exit 1
fi
echo "Compilation OK!"

cd ..
rm -r data

export LC_CTYPE=en_US.UTF-8 export LC_ALL=en_US.UTF-8

# file including lineage functions
cp source/src/backend/catalog/lineage.sql server/share/

./server/bin/initdb -D data
if [[ $? -ne 0 ]] ; then
	echo "Initialization error!"
    exit 1
fi
echo "Initialization OK!"


./server/bin/pg_ctl -D data -o "-F -p 5477" start
if [[ $? -ne 0 ]] ; then
  echo "Error starting porstgreSQL!"
	exit 1
fi

sleep 3

./server/bin/createdb -p 5477 tpdb
if [[ $? -ne 0 ]] ; then
	echo "Error creating database!"
	exit 1 
fi

./server/bin/psql -p 5477 -d tpdb -f data.sql
if [[ $? -ne 0 ]] ; then
	echo "Error creating data!"
	exit 1 
fi

./server/bin/pg_ctl -D data -o "-F -p 5477" stop
if [[ $? -ne 0 ]] ; then
  echo "Error starting porstgreSQL!"
	exit 1
fi




1) Read-write permissions are lost when updating the svn folder so potentially, before compiling postgreSQL, permissions need to be given to a set of files as follows:

chmod +x slow-build.sh
chmod +x start.sh
chmod +x stop.sh
chmod +x query.sh
chmod +x source/config.status

2) Compile PostgreSQL and start a new database including the relations in data.sql:
./slow-build.sh

3) Start the server and open psql to submit queries.
./start.sh
./query.sh

4) Stop the server using:
./stop.sh


5) Data is loaded during slow-build.sh from the file data.sql and examples of queries can also be found in this file. 


NOTE: Projection is not implemented and thus, when joins are performed, only * can be used as a target in the select.  


If problems occur, check your gcc version: gcc -v; vi ~/.bashrc;
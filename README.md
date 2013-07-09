Couchbase Mongo Importer (alpha)
================================

The Couchbase Mongo Importer tool allows you to:

* copy the data from your MongoDB Collection into a Couchbase Cluster
* export MongoDB documents into JSON files that could be loaded using [cbdocloader tool](http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-admin-cmdline-cbdocloader.html)

You can download the JAR file from [here](http://goo.gl/dfXSb)

### How to copy your data from MongoDB to Couchbase?

Once you have downloaded the JAR you can run the following commands:

Show Help
	
	java -jar cbmgimporter.jar -h
	

**Copy data from MongoDB to Couchbase directly:
**

	java -jar cbmgimporter.jar -n http://127.0.0.1:8091 -b cbbucket -mh 127.0.0.1 -db test 
	
	

**Generate JSON files for cbdocloader
**

Export all the collections from the test database:

	java -jar cbmgimporter.jar -mh 127.0.0.1 -db test -o ~/EXPORT/
	


Export the "employees" collection from the test database

	java -jar cbmgimporter.jar -mh 127.0.0.1 -db test -o ~/EXPORT/ -c employees
	

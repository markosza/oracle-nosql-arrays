# oracle-nosql-arrays

This repository contains examples about how to write and optimize queries
on data that contain nested arrays in the Oracle NoSQL Database System.

Repository contents:
--------------------

- nested_arrays.odt and nested_arrays.pdf
  Textual description and analysis of the example queries and the indexes
  used by these queries.

- doc1.json, doc2.json, doc3.json, and doc4.json
  Four data files, each containing a sample row for the "Users" table that is
  used in the examples.

- country_genre.idx, country_showid_date.idx, showid.idx, showid_minwatched.idx,
  and showid_seasonNum_minWatched.idx
  Files showing the contents of the indexes used in the examples. The contents
  correspond to the 4 sample rows.

- NestedArraysDemo.java
  A java application program that creates the Users table and the indexes, loads
  the table with the sample rows, and executes the example queries showing the
  results of each. The program uses the Oracle NoSQL java driver and runs over
  the cloud simulator. The java driver can be downloaded from here:
  https://github.com/oracle/nosql-java-sdk

- oracle-nosql-cloud-simulator-5.6.3.tar.gz
  The cloud simulator package (tared and zipped). Normally, cloudsim can be
  downloaded from https://www.oracle.com/downloads/cloud/nosql-cloud-sdk-downloads.html.
  However, the version stored there currently is not the latest one and does
  not support the unnesting feature that is needed by some of the examples.


Compiling and executing the NestedArraysDemo program:
-----------------------------------------------------

Let <root-dir> be the root directory of this repository and <driver-dir> be the
directory containing the nosqldriver.jar. Use the following commands to compile
the program:

cd <root-dir>
javac -cp <driver-dir>/nosqldriver.jar NestedArraysDemo.java

To run cloudsim, unzip and untar oracle-nosql-cloud-simulator-5.6.3.tar.gz
inside <root-dir>. Then run the following commands:

cd oracle-nosql-cloud-simulator-5.6.3
./runCloudSim -root store &

To execute the program use the following commands:

cd <root-dir>
java -cp .:<driver-dir>/nosqldriver.jar NestedArraysDemo http://localhost:8080



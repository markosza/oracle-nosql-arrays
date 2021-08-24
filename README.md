# oracle-nosql-arrays

This repository contains examples about how to write and optimize queries
on data that contain nested arrays in the Oracle NoSQL Database System.
Specifically it contains the following files:

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
  results of each. The comments in the file describe how to compile and run the
  program.


  

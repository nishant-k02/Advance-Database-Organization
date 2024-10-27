**Readme File**

Simple Record Manager \- CS 525 Advanced Database Organization

**\# Overview**

This project implements a simple record manager that handles tables with a fixed schema. Clients can perform the following operations:

\- Insert records  
\- Delete records  
\- Update records  
\- Scan through records in a table

The record manager works on top of the previously developed storage and buffer managers. This assignment supports fixed-length records and tables stored in separate page files.

 **\# Running the Program**

1\. Open the terminal and navigate to the project root.  
2\. Clean previous builds

- make clean

         
3\. Compile the project:

- make

         
4\. Run the tests:  
       

- ./test\_assign3

         
5\. To run tests for expressions:  
       

- ./test\_expr

**\#Functionalities**

**1\. Table and Manager Operations**  
\-  initRecordManager() : Initializes the record manager.  
\-  shutdownRecordManager() : Shuts down the record manager.  
\-  createTable() : Creates a new table with a given schema and free space information.  
\-  closeTable() : Closes an opened table.  
\-  deleteTable() : Deletes a table using \`destroyPageFile()\`.  
\-  getNumTuples() : Returns the number of records (tuples) in the table.

 **2\. Record Operations**  
\-  insertRecord() : Inserts a new record into a specified page and slot.  
\-  deleteRecord() : Deletes a record at the specified page and slot.  
\-  updateRecord() : Updates an existing record at a given page and slot.  
\-  getRecord() : Retrieves a record based on the page and slot.

**3\. Scans**  
\- startScan() : Initiates a scan based on a condition defined using the \`RM\_ScanHandle\` data structure.  
\-  next() : Returns the next tuple satisfying the criteria in the scan.  
\-  closeScan() : Cleans up resources associated with the scan.

**4\. Schema Management**  
\- getRecordSize()  : Returns the size of records for a schema or throws an error if the    schema is missing.  
\-   createSchema()  : Defines a new schema with attributes, types, and keys.  
\-   freeSchema()  : Frees memory associated with a schema.

 **5\. Record and Attribute Management**  
\-   createRecord()  : Creates a record and initializes it with default values.  
\-   freeRecord()  : Frees memory allocated to a record.  
\-   getAttr()  : Retrieves the value of a record   s attribute.  
\-   setAttr()  : Sets the value of a record   s attribute.

      
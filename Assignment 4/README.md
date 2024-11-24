# CS 525 Advanced Database Organization

## **Assignment 4 - B+ Tree ReadMe**

In this assignment we are implementing a B+-tree index. The B+-Tree supports efficient indexing operations, such as inserting, deleting, and searching for keys. Each node in the tree occupies a single page, managed via a buffer manager, and the tree supports dynamic fan-out adjustments for debugging purposes. The B+-Tree only supports integer keys (DT\_INT), and each key points to a record identified by a *Record ID (RID)*.


**How to Run:**

1\. Open the terminal and navigate to the project root. 

2\. Clean previous builds 

\-	`make clean`

3\. Compile the project:

\- `make`

4\. Run the tests: 

\-	`./test_assign4` 

5\. To run tests for expressions: 

\-	`./test_expr`

# **Functionalities**

**1. Index Manager Functions:** 

- initIndexManager(): Initializes the index manager.
- shutdownIndexManager(): Shuts down the index manager and frees resources.

**2. B+ Tree Functions:** 

- createBtree(): Creates a B+-Tree index with a specified key type and fan-out order.
- openBtree(): Opens an existing B+-Tree index for access.
- closeBtree(): Closes an open B+-Tree index and ensures all modified pages are flushed to disk.
- deleteBtree(): Deletes an existing B+-Tree index file.

**3. Information about B+ Tree:** 

- getNumNodes(): Get Number of nodes in the B+-Tree.
- getNumEntries(): Get Number of entries in the B+-Tree.
- getKeyType(): Get Key type in the B+-Tree.

**4. Key Functions:** 

- findKey(): Finds and returns the RID for a given key in the B+-Tree.
- insertKey(): Inserts a new key-RID pair into the B+-Tree.
- deleteKey(): Removes a key-RID pair from the B+-Tree.

**5.** **Tree Scan Functions:** 

- openTreeScan(): Opens a scan to traverse the B+-Tree entries in sorted order.
- nextEntry(): Retrieves the next entry in the tree scan.
- closeTreeScan(): Closes the open tree scan.

**6. Debug Functions:** 

- printTree(): Produces a string representation of the B+-Tree in depth-first pre-order format. Useful for debugging and test validation.



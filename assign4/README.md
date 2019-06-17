## Assignment 4 - B+-Tree

#### Group Members
- Manuel Alaman Escolano
- Andrew Hile
- Rodrigo Lopez De Toledo Soler
- Eloy Ramirez Hernanz

#### Design Choies
- Struct BT_MgmtData houses the index information, including a pointer to the buffer pool.
- Struct BT_ScanMgmtData houses the information relevant to the BT_ScanHandle struct.
- Struct Node houses the contents of a particular node.
- Additional functions 'readNode' and 'writeNode' built to interface directly with the index file rather than storing the index in memory and also perform serializing/deserializing operations respectively.
- The first page of the index file contains the metadata for the index.

### Build Instructions
1. Comment out the second test for removing nodes in 'main' in test_assign4_1.c
2. Run 'make' to build the executable
3. Run './test_assign4_1'

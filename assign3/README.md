## Assignment 3 - Record Manager

#### Group Members
- Manuel Alaman Escolano
- Andrew Hile
- Rodrigo Lopez De Toledo Soler
- Eloy Ramirez Hernanz

#### Design Choies
- Struct RM_MgmtData houses the record manager information, including a pointer to the buffer pool.
- Struct RM_ScanMgmtData houses the information relevant to the RM_ScanHandle struct.
- Built-in functions *fopen*, *fseek*, *fread*, *fwrite*, and *fclose* execute the desired interactions with the database file.
- The first page of a file contains serialized versions of the schema, the number of tuples in the file, and all the free slots in the file. These items are delimited by '&'.
- The rest of the file contains the records delimited by '&'.
- Deleted records are overwritten with '@'.
- Deserializing operations are built into the openTable and getRecord functions.
- The location of any free slots is maintained in the RM_MgmtData struct as an array of the RID struct. The first element of this array is always the slot at the very end of the file.

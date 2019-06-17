## Assignment 2 - Buffer Manager

#### Group Members
- Manuel Alaman Escolano
- Andrew Hile
- Rodrigo Lopez De Toledo Soler
- Eloy Ramirez Hernanz

#### Design Choies
- Struct BM_MgmtData houses the buffer pool information, including a pointer to the buffer contents.
- Struct BM_Frame houses the the information about a given frame; the buffer pool is a BM_Frame array.
- Built-in functions *fopen*, *fseek*, *fread*, *fwrite*, and *fclose* execute the desired interactions with the database file.
- Replacement strategy FIFO was implemented by use of a tracking circular queue index.
- Replacement strategy LRU was implemented by adding an underlying doubly-linked list structure to the buffer.

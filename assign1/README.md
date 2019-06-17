## Assignment 1 - Storage Manager

#### Group Members
- Manuel Alaman Escolano
- Andrew Hile
- Rodrigo Lopez De Toledo Soler
- Eloy Ramirez Hernanz

#### Design Choies
- Built-in functions *fopen*, *fseek*, *fread*, *fwrite*, and *fclose* execute the desired interactions with the database file.
- Metadata (totalNumPages) is stored as an *SM_Header* at the beginning of the file, and is only updated when new blocks are appended to the file.

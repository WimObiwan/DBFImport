# DBFImport
A simple tool to import old DBF files into SQL Server.
DBF files are used by dBase (Aston Tate), FoxPro (Fox Software - Microsoft), ... 

# Usage

    DBFImporter.exe [path] [server] [database]
    DBFImporter.exe [path] [conn]

       path      file path, directory path or file mask of the DBF file(s)
       server    SQL Server hostname or IP address (optionally including instance name)
       database  name of existing database
       conn      valid connection string

The database must be an existing database.  Every DBF file is imported into a database table with the same name (if the table exists, it is dropped first).

# Example

    DBFImporter.exe "c:\My DBF files\*.DBF" DEVSERVER\SQL2017 ImportedDbfFiles
    
# Known issues

 * Not alle datatypes from the xBase flavors are implemented.  Any issues or pull requests are welcome.
 * Memo types (and DBT files) are not yet implemented.

# References

 * https://en.wikipedia.org/wiki/.dbf
 * http://www.manmrk.net/tutorials/database/xbase/ 

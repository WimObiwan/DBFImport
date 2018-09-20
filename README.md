# DBFImport
A simple tool to import old DBF files into SQL Server

# Usage

    DBFImporter.exe [path] [server] [database]
    DBFImporter.exe [path] [conn]

       path      file path, directory path or file mask of the DBF file(s)
       server    SQL Server hostname or IP address (optionally including instance name)
       database  name of existing database
       conn      valid connection string

# Example

    DBFImporter.exe "c:\My DBF files\*.DBF" DEVSERVER\SQL2017 ImportedDbfFiles
    
# References

 * https://en.wikipedia.org/wiki/.dbf
 * http://www.manmrk.net/tutorials/database/xbase/ 

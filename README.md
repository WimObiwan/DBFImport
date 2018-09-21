# DBFImport
A simple tool to import old DBF files into SQL Server.
DBF files are used by dBase (Aston Tate), FoxPro (Fox Software - Microsoft), ...
Built on DotNet Core 2.1, works on Windows, Linux, Mac.

# Usage

    dotnet DBFImport.dll [path] [server] [database]
    dotnet DBFImport.dll [path] [conn]

       path      file path, directory path or file mask of the DBF file(s)
       server    SQL Server hostname or IP address (optionally including instance name)
       database  name of existing database
       conn      valid connection string

The database must be an existing database.  Every DBF file is imported into a database table with the same name (if the table exists, it is dropped first).

# Example

    DBFImporter.exe "c:\My DBF files\*.DBF" DEVSERVER\SQL2017 ImportedDbfFiles

# Download & installation
 
 * Requires dotnet Core 2.1 (available on Windows, Linux, Mac)
 * https://github.com/WimObiwan/DBFImport/releases/latest
 * Download & unzip the asset: DBFImport.zip

# Known issues & To do's
Any [issues](https://github.com/WimObiwan/DBFImport/issues) or [pull requests](https://github.com/WimObiwan/DBFImport/pulls) are welcome.

 * Not alle datatypes from the xBase flavors are implemented.
 * Memo types (and DBT files) are not yet implemented.
 * Currently only SQL Server is supported

# References

 * https://en.wikipedia.org/wiki/.dbf
 * http://www.manmrk.net/tutorials/database/xbase/ 

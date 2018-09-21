# DBFImport
A simple tool to import old DBF files into SQL Server.

DBF files were/are used by 
 * dBase (Ashton Tate)
 * FoxBASE/FoxPro (Fox Software) / Visual FoxPro (Microsoft)
 * Clipper (Nantucket / Computer Associates)
 * ...

Built on DotNet Core 2.1, works on Windows, Linux, Mac.

# Usage

    dotnet DBFImport.dll [dbfpath] [server] [database]
    dotnet DBFImport.dll [dbfpath] [conn]

       dbfpath   file path, directory path or file mask of the DBF file(s)
       server    SQL Server hostname or IP address (optionally including instance name)
       database  name of existing database
       conn      valid connection string

The database must be an existing database.  Every DBF file is imported into a database table with the same name (if the table exists, it is dropped first).

# Example

    dotnet DBFImport.dll "c:\My DBF files\*.DBF" DEVSERVER\SQL2017 ImportedDbfFiles

# Download & installation
 
 * Requires dotnet Core 2.1 (available on Windows, Linux, Mac)
 * https://github.com/WimObiwan/DBFImport/releases/latest
 * Download & unzip the asset: DBFImport.zip

# Known issues & To do's

Any [issues](https://github.com/WimObiwan/DBFImport/issues) or [pull requests](https://github.com/WimObiwan/DBFImport/pulls) are welcome.

 * Not alle datatypes from the xBase flavors are implemented.
 * Memo types (and DBT files) are not yet implemented.
 * Currently only SQL Server is supported (any flavor/version should work)

# References

 * https://en.wikipedia.org/wiki/.dbf
 * http://www.manmrk.net/tutorials/database/xbase/ 

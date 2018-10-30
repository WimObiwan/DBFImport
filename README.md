# DBFImport
A simple tool to import old DBF files into SQL Server.

DBF files were/are used by 
 * dBase (Ashton Tate)
 * FoxBASE/FoxPro (Fox Software) / Visual FoxPro (Microsoft)
 * Clipper (Nantucket / Computer Associates)
 * ...

Built on DotNet Core 2.1, works on Windows, Linux, Mac.

# Usage

    dotnet DBFImport.dll -p <dbfpath> -s <server> -d <database> [--codepage <cp>] [--nobulkcopy]
    dotnet DBFImport.dll -p <dbfpath> -c <conn> [--codepage <cp>] [--nobulkcopy]
    
       -p, --path                Required. Path to DBF file(s)
       -s, --server              Required. SQL Server (and instance)
       -d, --database            Required. Database name
       -c, --connectionstring    Required. Database connection string
       --codepage                Code page for decoding text
       --nobulkcopy              Use much slower 'SQL Command' interface, instead of 'SQL BulkCopy'
       --help                    Display this help screen.
       --version                 Display version information.

The database must be an existing database.  Every DBF file is imported into a database table with the same name (if the table exists, it is dropped first).

# Examples

    dotnet DBFImport.dll "c:\My DBF files\*.DBF" DEVSERVER\SQL2017 ImportedDbfFiles
    #Imports DBF files:
    dotnet DBFImport.dll --path "c:\Data\My DBF files\*.DBF" `
       --server DEVSERVER\SQL2017 --database ImportedDbfFiles
    #Imports DBF files, and decode text using code page 1252:
    dotnet DBFImport.dll --path "c:\Data\My DBF files\*.DBF" `
       --server DEVSERVER\SQL2017  --database ImportedDbfFiles --codepage 1252 
    #Imports DBF files, and connect to SQL Server using connection string:
    dotnet DBFImport.dll --path "c:\Data\My DBF files\*.DBF" `
       --connectionstring "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword"

# Download & installation
 
 * Requires dotnet Core 2.1 (available on Windows, Linux, Mac)
 * https://github.com/WimObiwan/DBFImport/releases/latest
 * Download & unzip the asset: DBFImport.zip

# Typical speed characteristics

For what it's worth, this is a real situation:

 * High-end laptop with SSD disk
 * Import of 714 DBF files, containing 4868381 records (excluding those marked for deletion)
 * Import into a local SQL Server 2017 Developer Edition.
 * Using BulkCopy, it takes 2:16.825 (136 seconds)
 * Using SQL Command (`--nobulkcopy`), it takes 16:49.497

# Known issues & To do's

Any feedback ([issues](https://github.com/WimObiwan/DBFImport/issues) or [pull requests](https://github.com/WimObiwan/DBFImport/pulls)) is welcome!

 * Not alle datatypes from the xBase flavors are implemented.
 * Memo types (and DBT files) are not (yet) implemented.
 * Currently only SQL Server is supported.

# Alternatives

There are alternatives:

 * [Microsoft OLE DB Provider for Visual FoxPro 9.0 (VfpOleDB.dll)](https://www.microsoft.com/en-us/download/details.aspx?id=14839)
 * [Visual FoxPro ODBC Driver](https://docs.microsoft.com/en-us/sql/odbc/microsoft/visual-foxpro-odbc-driver?view=sql-server-2017)
 * Commercial products
 
 For me these were insufficient because of:
 
 * incompatibilities between 32-bit drivers and 64-bit SQL Sever
 * instabilities of SQL Server when using some of the drivers in linked servers

# References

 * https://en.wikipedia.org/wiki/.dbf
 * http://www.manmrk.net/tutorials/database/xbase/ 

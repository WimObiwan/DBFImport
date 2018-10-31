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

The database must be an existing database.  Every DBF file is imported into a database table with the same name (if the table exists, it is dropped first).  A dot ('.') is displayed per 1000 records that are INSERTed in the database.

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

# Typical output

    PS C:\> dotnet .\DBFImport.dll `
    >>     --path 'c:\Data\My DBF files\*.DBF' --codepage 1252 `
    >>     --server 'DEVSERVER\SQL2017' --database 'ImportedDbfFiles'
    Processing c:\Data\My DBF files\ABALANS.DBF...
      LastUpdate:       16/08/2018
      Fields:           14
      Records:          5
      Importing:
      Inserted:         1
      MarkedAsDeleted:  4
      Duration:         00:00:00.9591436
    Processing c:\Data\My DBF files\ANALYT.dbf...
      LastUpdate:       26/10/2018
      Fields:           22
      Records:          33496
      Importing:        ................................
      Inserted:         32402
      MarkedAsDeleted:  1094
      Duration:         00:00:00.4355719
    ...

    Import finished.
    Statistics:
      Records:          4869097
      Succeeded files:  714
      Failed files:     0
      Total Duration:   00:02:46.6250966

# Download & installation
 
 * Requires dotnet Core 2.1 (available on Windows, Linux, Mac).  
   * Go to https://getdotnet.org, click "Download", click "Download .net Core Runtime".
   * Or install using Chocolatey: `choco install dotnetcore-runtime`
 * Download & unzip the zip-file DBFImport.zip from https://github.com/WimObiwan/DBFImport/releases/latest

# Typical speed characteristics

For what it's worth, this is a real situation:

 * DBFImport.dll running on high-end laptop with SSD disk, Intel i7, 16 GB RAM.
 * DBFImport.dll compiled in Release mode, running on Windows 10 (1804), using dotnet Core 2.1.205 SDK.
 * Import of 714 local DBF files, total size 2493014358 bytes (2.32 GiB)
 * Containing 4868381 records (excluding those marked for deletion)
 * {1} Import into a local SQL Server 2017 Developer Edition.
 * {2} Import into a remote SQL Server 2016 Standard Edition (server specs unknown, virtualized, hosted in a datacenter)
 * In every test, the existing (filled) tabled was dropped and recreated.

|                              | {1} Local SQL Server 2017            | {2} Remote SQL Server 2016 |
| ---------------------------- | ------------------------------------ | -------------------------- |
| SQL Command (`--nobulkcopy`) | 16:11.421 (971 s)<br>4823 records/s<br>2.36 MiB/s | (\*) 50.5 hours<br>27 records/s<br>13.39 KiB/s |
| SQL BulkCopy                 | 2:05.705 (126 s)<br>38729 records/s<br>18.91 MiB/s  | 9:48.394 (588 s)<br>8274 records/s<br>4.04 MiB/s |
| Speedup of BulkCopy          | x7.73                                | 308.98x                            |

(\*) This test took ages, these numbers were obtained by extrapolation.

Resource utilization of dotnet.exe:

|                              | {1} Local SQL Server 2017            |         | {2} Remote SQL Server 2016  |          |
|                              | CPU                                  | Memory  | CPU                         | Memory  |
| ---------------------------- | ------------------------------------ | ------- | --------------------------- | ------- |
| SQL Command (`--nobulkcopy`) | 4-8%<br>(during 16 minutes)          | 12.2 MB | 0%                          | 10.9 MB |
| SQL BulkCopy                 | 12% constantly<br>(during 2 minutes) | 12.1 MB | 1-4%<br>(during 10 minutes) | 11.7 MB |

# Impact of the option `--nobulkcopy`

* When using BulkCopy, alle INSERTs are done in Batches of 10000 records.  When an error happens, the current batch is rolled back, and the BuldCopy is aborted.  All records INSERTed during previous batches will be present in the table.
* When not using BulkCopy, every INSERT is done (separately using a prepared SQL Command), inside a single transaction.  This means that when an error occurs, the complete transaction is rolled back, and no records will be present in the database.

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

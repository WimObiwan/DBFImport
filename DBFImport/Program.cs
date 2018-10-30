using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Net.Http.Headers;
using System.Reflection.Metadata;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using CommandLine;
using CommandLine.Text;

namespace DBFImport
{
    class Program
    {

        class Options
        {
            [Option('p', "path", Required = true, HelpText = "Path to DBF file(s)")]
            public string DbfPath { get; set; }

            [Option('s', "server", SetName = "server&db", Required = true, HelpText = "SQL Server (and instance)")]
            public string Server { get; set; }

            [Option('d', "database", SetName = "server&db", Required = true, HelpText = "Database name")]
            public string Database { get; set; }

            [Option('c', "connectionstring", SetName = "connstring", Required = true, HelpText = "Database connection string")]
            public string ConnectionString { get; set; }

            [Option("codepage", Required = false, HelpText = "Code page for decoding text")]
            public int CodePage { get; set; }

            [Option("nobulkcopy", Required = false, HelpText = "Use much slower 'SQL Command' interface, instead of 'SQL BulkCopy'")]
            public bool NoBulkCopy { get; set; }

            [Usage]
            public static IEnumerable<Example> Examples
            {
                get
                {
                    return new List<Example>() {
                        new Example("Imports DBF files",
                            new Options
                            {
                                DbfPath = @"c:\Data\My DBF files\*.DBF",
                                Server = @"DEVSERVER\SQL2017",
                                Database = "ImportedDbfFiles",
                            }),
                        new Example("Imports DBF files, and decode text using code page 1252",
                            new Options
                            {
                                DbfPath = @"c:\Data\My DBF files\*.DBF",
                                Server = @"DEVSERVER\SQL2017",
                                Database = "ImportedDbfFiles",
                                CodePage = 1252
                            }),
                        new Example("Imports DBF files, and connect to SQL Server using connection string",
                            new Options
                            {
                                DbfPath = @"c:\Data\My DBF files\*.DBF",
                                ConnectionString = @"Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword",
                            }),
                    };
                }
            }
        }

        public static int Main(string[] args)
        {
            return Parser.Default
                .ParseArguments<Options>(args)
                .MapResult(
                    o => RunWithOptions(o),
                    errs => 1);
        }

        private static int RunWithOptions(Options options)
        {
            string path = options.DbfPath;

            string connectionString = options.ConnectionString;
            if (string.IsNullOrEmpty(connectionString))
            {
                SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder();
                connectionStringBuilder.DataSource = options.Server;
                connectionStringBuilder.InitialCatalog = options.Database;
                connectionStringBuilder.IntegratedSecurity = true;
                connectionString = connectionStringBuilder.ConnectionString;
            }

            int codepage = options.CodePage;
            bool noBulkCopy = options.NoBulkCopy;

            int failedFiles = 0;
            int succeededFiles = 0;
            Stopwatch sw = Stopwatch.StartNew();

            int totalInsertCount = 0;
            if (File.Exists(path))
            {
                int insertCount = ProcessFile(path, connectionString, codepage, noBulkCopy);
                if (insertCount >= 0)
                {
                    totalInsertCount += insertCount;
                    succeededFiles++;
                }
                else
                {
                    failedFiles++;
                }
            }
            else
            {
                string mask;
                if (Directory.Exists(path))
                {
                    mask = "*.DBF";
                }
                else
                {
                    mask = Path.GetFileName(path);
                    path = Path.GetDirectoryName(path);
                }

                foreach (var file in Directory.EnumerateFiles(path, mask))
                {
                    int insertCount = ProcessFile(file, connectionString, codepage, noBulkCopy);
                    if (insertCount >= 0)
                    {
                        totalInsertCount += insertCount;
                        succeededFiles++;
                    }
                    else
                    {
                        failedFiles++;
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Import finished.");
            Console.WriteLine("Statistics:");
            Console.WriteLine($"  Records:          {totalInsertCount}");
            Console.WriteLine($"  Succeeded files:  {succeededFiles}");
            Console.WriteLine($"  Failed files:     {failedFiles}");
            Console.WriteLine($"  Total Duration:   {sw.Elapsed}");

            return failedFiles;
        }

        static int ProcessFile(string filename, string connectionString, int codepage, bool noBulkCopy)
        {
            Console.WriteLine($"Processing {filename}...");
            Stopwatch sw = Stopwatch.StartNew();
            try
            {
                using (DbfFileStream dbfFileStream = new DbfFileStream(filename, codepage))
                {
                    string table = Path.GetFileNameWithoutExtension(filename);

                    Console.WriteLine($"  LastUpdate:       {dbfFileStream.Header.LastUpdate.ToShortDateString()}");
                    Console.WriteLine($"  Fields:           {dbfFileStream.Header.FieldCount}");
                    Console.WriteLine($"  Records:          {dbfFileStream.Header.RecordCount}");
                    Console.Write("  Importing:        ");

                    (int insertCount, int deletedCount) = 
                        CreateTable(connectionString, table, dbfFileStream.FieldDescriptors, dbfFileStream.Records, noBulkCopy);
                    Console.WriteLine($"  Inserted:         {insertCount}");
                    Console.WriteLine($"  MarkedAsDeleted:  {deletedCount}");
                    Console.WriteLine($"  Duration:         {sw.Elapsed}");

                    return insertCount;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to process file {filename}");
                Console.WriteLine($"   Exception: {e.Message}");
                while (e.InnerException != null)
                {
                    e = e.InnerException;
                    Console.WriteLine($"       Inner: {e.Message}");
                }

#if DEBUG
                throw;
#endif

                return -1;
            }
        }

        private static (int insertedCount, int deletedCount) CreateTable(string connectionString, string table, 
            IReadOnlyList<DbfFieldDescriptor> fieldDescriptors, IEnumerable<DbfRecord> records,
            bool noBulkCopy)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                try
                {
                    conn.Open();
                }
                catch (Exception e)
                {
                    throw new Exception("Failed to connect to database", e);
                }

                try
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE [{table}];";
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to drop existing table {table}", e);
                }

                try
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.AppendLine($"CREATE TABLE [{table}] (");
                        bool first = true;
                        foreach (var fieldDescriptor in fieldDescriptors)
                        {
                            if (first)
                                first = false;
                            else
                                sb.Append(", ");
                            sb.AppendLine($"[{fieldDescriptor.Name}] {fieldDescriptor.GetSqlDataType()}");
                        }
                        sb.AppendLine($")");
                        cmd.CommandText = sb.ToString();
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to create table {table}", e);
                }

                try
                {
                    if (noBulkCopy)
                        return FillTableUsingSqlCommand(conn, table, fieldDescriptors, records);
                    else
                        return FillTableUsingBulkCopy(conn, table, fieldDescriptors, records);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to fill table {table}", e);
                }
            }
        }

        private static (int insertedCount, int deletedCount) FillTableUsingBulkCopy(
            SqlConnection conn, string table, IReadOnlyList<DbfFieldDescriptor> fieldDescriptors,
            IEnumerable<DbfRecord> records)
        {
            using (SqlBulkCopy bcp =  new SqlBulkCopy(conn))
            {
                bcp.DestinationTableName = $"[{table}]";
                bcp.BulkCopyTimeout = 1800;

                //DataTable dataTable = new DataTable();

                //foreach (var fieldDescriptor in fieldDescriptors)
                //{
                //    dataTable.Columns.Add(fieldDescriptor.Name, fieldDescriptor.GetDataType());
                //}

                //int insertCount = 0, deletedCount = 0;
                //foreach (var record in records)
                //{
                //    if (record.Deleted)
                //    {
                //        deletedCount++;
                //        continue;
                //    }

                //    var row = dataTable.NewRow();

                //    for (int col = 0; col < fieldDescriptors.Count; col++)
                //    {
                //        row[col] = record.Fields[col] ?? DBNull.Value; ;
                //    }

                //    dataTable.Rows.Add(row);
                //    insertCount++;

                //    if (insertCount % 1000 == 0)
                //    {
                //        Console.Write('.');
                //    }
                //}
                //Console.WriteLine();

                //bcp.WriteToServer(dataReader);

                //return (dataReader.Inserted, dataReader.Deleted);

                DataReader dataReader = new DataReader(fieldDescriptors, records);

                try
                {
                    bcp.BatchSize = 10000;
                    bcp.NotifyAfter = 1000;
                    bcp.SqlRowsCopied += delegate (object sender, SqlRowsCopiedEventArgs args) { Console.Write('.'); };
                    bcp.WriteToServer(dataReader);
                }
                finally 
                {
                    Console.WriteLine();
                }

                return (dataReader.Inserted, dataReader.Deleted);
            }
        }

        private static (int insertedCount, int deletedCount) FillTableUsingSqlCommand(
            SqlConnection conn, string table, IReadOnlyList<DbfFieldDescriptor> fieldDescriptors,
            IEnumerable<DbfRecord> records)
        {
            using (SqlCommand cmd = conn.CreateCommand())
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine($"INSERT INTO [{table}] (");
                bool first = true;
                foreach (var fieldDescriptor in fieldDescriptors)
                {
                    if (first)
                        first = false;
                    else
                        sb.Append(", ");
                    sb.Append($"[{fieldDescriptor.Name}]");
                }

                sb.AppendLine($") VALUES (");
                int no = 0;
                first = true;
                foreach (var fieldDescriptor in fieldDescriptors)
                {
                    if (first)
                        first = false;
                    else
                        sb.Append(", ");
                    sb.Append($"@p{no}");

                    cmd.Parameters.Add(fieldDescriptor.GetSqlParameter($"@p{no}"));

                    no++;
                }

                sb.AppendLine($")");
                cmd.CommandText = sb.ToString();
                int insertCount = 0, deletedCount = 0;
                using (var transaction = conn.BeginTransaction())
                {
                    foreach (var record in records)
                    {
                        if (record.Deleted)
                        {
                            deletedCount++;
                            continue;
                        }

                        try
                        {
                            no = 0;
                            foreach (var field in record.Fields)
                            {
                                cmd.Parameters[$"@p{no}"].Value = field ?? DBNull.Value;
                                no++;
                            }

                            cmd.Transaction = transaction;
                            cmd.ExecuteNonQuery();
                            insertCount++;
                        }
                        catch (Exception e)
                        {
                            throw new Exception(
                                $"Failed to insert record #{record.RecordNo + 1} into database, {insertCount} already inserted",
                                e);
                        }

                        if (insertCount % 1000 == 0)
                        {
                            Console.Write('.');
                        }
                    }

                    Console.WriteLine();
                    transaction.Commit();
                }

                return (insertCount, deletedCount);
            }
        }
    }
}

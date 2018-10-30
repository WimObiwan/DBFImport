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

            int failedFiles = 0;
            int succeededFiles = 0;
            Stopwatch sw = Stopwatch.StartNew();

            if (File.Exists(path))
            {
                if (ProcessFile(path, connectionString))
                    succeededFiles++;
                else
                    failedFiles++;
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
                    if (ProcessFile(file, connectionString))
                        succeededFiles++;
                    else
                        failedFiles++;
                }
            }

            Console.WriteLine($"Succeeded files: {succeededFiles}");
            Console.WriteLine($"Failed files:    {failedFiles}");
            Console.WriteLine($"Total Duration:  {sw.Elapsed}");

            return failedFiles;
        }

        static bool ProcessFile(string filename, string connectionString)
        {
            Console.WriteLine($"Processing {filename}...");
            Stopwatch sw = Stopwatch.StartNew();
            bool result;
            try
            {
                using (DbfFileStream dbfFileStream = new DbfFileStream(filename))
                {
                    string table = Path.GetFileNameWithoutExtension(filename);

                    Console.WriteLine($"  LastUpdate:       {dbfFileStream.Header.LastUpdate.ToShortDateString()}");
                    Console.WriteLine($"  Fields:           {dbfFileStream.Header.FieldCount}");
                    Console.WriteLine($"  Records:          {dbfFileStream.Header.RecordCount}");

                    (int insertCount, int deletedCount) = CreateTable(connectionString, table, dbfFileStream.FieldDescriptors, dbfFileStream.Records);
                    Console.WriteLine($"  Inserted:         {insertCount}");
                    Console.WriteLine($"  MarkedAsDeleted:  {deletedCount}");
                }

                result = true;
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

                result = false;
            }

            Console.WriteLine($"  Duration:   {sw.Elapsed}");

            return result;
        }

        private static (int insertedCount, int deletedCount) CreateTable(string connectionString, string table, 
            IReadOnlyList<DbfFieldDescriptor> fieldDescriptors,
            IEnumerable<DbfRecord> records)
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
                    throw new Exception($"Failed to drop existing table ${table}", e);
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
                    throw new Exception($"Failed to create table ${table}", e);
                }

                try
                {
                    //return FillTableUsingSqlCommand(conn, table, fieldDescriptors, records);
                    return FillTableUsingBulkCopy(conn, table, fieldDescriptors, records);
                }
                catch (Exception e)
                {
                    throw new Exception($"Failed to fill table ${table}", e);
                }
            }
        }

        private static (int insertedCount, int deletedCount) FillTableUsingBulkCopy(
            SqlConnection conn, string table, IReadOnlyList<DbfFieldDescriptor> fieldDescriptors,
            IEnumerable<DbfRecord> records)
        {
            using (SqlBulkCopy bcp =  new SqlBulkCopy(conn))
            {
                bcp.DestinationTableName = table;
                bcp.BulkCopyTimeout = 1800;

                DataTable dataTable = new DataTable();

                foreach (var fieldDescriptor in fieldDescriptors)
                {
                    dataTable.Columns.Add(fieldDescriptor.Name, fieldDescriptor.GetDataType());
                }

                int insertCount = 0, deletedCount = 0;
                foreach (var record in records)
                {
                    if (record.Deleted)
                    {
                        deletedCount++;
                        continue;
                    }

                    var row = dataTable.NewRow();

                    for (int col = 0; col < fieldDescriptors.Count; col++)
                    {
                        row[col] = record.Fields[col] ?? DBNull.Value; ;
                    }

                    dataTable.Rows.Add(row);
                    insertCount++;
                }

                bcp.WriteToServer(dataTable);

                return (insertCount, deletedCount);

                //BcpRecordAdaptor dataTable = new BcpRecordAdaptor(fieldDescriptors, records);

                //bcp.WriteToServer(dataTable);

                //return (dataTable.InsertCount, dataTable.DeletedCount);
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
                    }
                    transaction.Commit();
                }

                return (insertCount, deletedCount);
            }
        }
    }
}

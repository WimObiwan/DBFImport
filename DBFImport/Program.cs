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

namespace DBFImport
{
    class Program
    {
        static int Main(string[] args)
        {
            Version version = Assembly.GetEntryAssembly().GetName().Version;
            Console.WriteLine($"DBFImporter {version.Major}.{version.Minor}.{version.Build}");

            if (args.Length < 2 || args.Length > 3)
            {
                Console.WriteLine();
                Console.WriteLine("Usage: DBFImporter.exe [path] [server] [database]");
                Console.WriteLine("Usage: DBFImporter.exe [path] [conn]");
                Console.WriteLine();
                Console.WriteLine("   path      file path, directory path or file mask of the DBF file(s)");
                Console.WriteLine("   server    SQL Server hostname or IP address (optionally including instance name)");
                Console.WriteLine("   database  name of existing database");
                Console.WriteLine("   conn      valid connection string");
                Console.WriteLine();
                Console.WriteLine(@"Example: DBFImporter.exe ""c:\My DBF files\*.DBF"" DEVSERVER\SQL2017 ImportedDbfFiles");
                return 1;
            }

            string path = args[0];
            string connectionString;
            if (args.Length == 3)
            {
                string server = args[1];
                string database = args[2];

                System.Data.SqlClient.SqlConnectionStringBuilder connectionStringBuilder =
                    new SqlConnectionStringBuilder();
                connectionStringBuilder.DataSource = server;
                connectionStringBuilder.InitialCatalog = database;
                connectionStringBuilder.IntegratedSecurity = true;
                connectionString = connectionStringBuilder.ConnectionString;
            }
            else
            {
                connectionString = args[1];
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

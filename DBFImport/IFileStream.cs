using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace DBFImport
{
    interface IFileStream : IDisposable
    {
        IHeader Header { get; }

        IReadOnlyList<IFieldDescriptor> FieldDescriptors { get; }

        IEnumerable<Record> Records { get; }
    }

    interface IHeader
    {
        DateTime LastUpdate { get; }
        int? RecordCount { get; }
        int FieldCount { get; }
    }

    interface IFieldDescriptor
    {
        int No { get; }
        string Name { get; }
        string GetSqlDataType();
        SqlParameter GetSqlParameter(string name);
    }

    class Record
    {
        public int RecordNo { get; set; }
        public object[] Fields { get; set; }
    }

}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace DBFImport
{
    interface IFileStream : IDisposable
    {
        Header Header { get; }

        IReadOnlyList<IFieldDescriptor> FieldDescriptors { get; }

        IEnumerable<Record> Records { get; }
    }

    class Header
    {
        public byte Version { get; set; }
        public DateTime LastUpdate { get; set; }
        public int RecordCount { get; set; }
        public short HeaderLength { get; set; }
        public short RecordLength { get; set; }

        public int FieldCount => (HeaderLength / 32 - 1);
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
        public bool Deleted { get; set; }
        public object[] Fields { get; set; }
    }

}

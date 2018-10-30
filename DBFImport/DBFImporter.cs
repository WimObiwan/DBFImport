using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net.Http.Headers;
using System.Text;

namespace DBFImport
{
    class DbfHeader
    {
        public byte Version { get; set; }
        public DateTime LastUpdate { get; set; }
        public int RecordCount { get; set; }
        public short HeaderLength { get; set; }
        public short RecordLength { get; set; }

        public int FieldCount => (HeaderLength / 32 - 1);
    }

    class DbfFieldDescriptor
    {
        public int No { get; set; }
        public string Name { get; set; }
        public char TypeChar { get; set; }
        public int Length { get; set; }
        public byte DecimalCount { get; set; }

        public Type GetDataType()
        {
            switch (TypeChar)
            {
                case 'C':
                    return typeof(string);
                case 'I':
                    return typeof(int);
                case 'N':
                    return typeof(decimal);
                case 'L':
                    return typeof(bool);
                case 'D':
                    return typeof(DateTime);
                case 'M':
                    return typeof(string); //?
                case 'T':
                    return typeof(DateTime);
                case 'W': //?
                    return typeof(string); //?
                case '0':
                    return typeof(int);
                case 'G':
                    return typeof(string); //?
                //case 'F':
                //    return "FLOAT";
                default:
                    throw new NotSupportedException();
            }
        }

        public string GetSqlDataType()
        {
            switch (TypeChar)
            {
                case 'C':
                    return $"VARCHAR({Length})";
                case 'I':
                    return "INT";
                case 'N':
                    return $"DECIMAL({Length + 1}, {DecimalCount})";
                case 'L':
                    return "BIT";
                case 'D':
                    return "DATETIME";
                case 'M':
                    return "VARCHAR(MAX)";
                case 'T':
                    return "DATETIME";
                case 'W': //?
                    return "VARCHAR(MAX)";
                case '0':
                    return "INT";
                case 'G':
                    return "VARCHAR(MAX)";
                case 'F':
                    return "FLOAT";
                default:
                    throw new NotSupportedException();
            }

        }

        internal SqlParameter GetSqlParameter(string name)
        {
            switch (TypeChar)
            {
                case 'C':
                    return new SqlParameter(name, SqlDbType.VarChar, Length);
                case 'I':
                    return new SqlParameter(name, SqlDbType.Int);
                case 'N':
                {
                    var par = new SqlParameter(name, SqlDbType.Decimal);
                    par.Precision = (byte)(Length + 3);
                    par.Scale = DecimalCount;
                    return par;
                }
                case 'L':
                    return new SqlParameter(name, SqlDbType.Bit);
                case 'D':
                    return new SqlParameter(name, SqlDbType.DateTime);
                case 'M':
                    return new SqlParameter(name, SqlDbType.VarChar, -1);
                case 'T':
                    return new SqlParameter(name, SqlDbType.DateTime);
                case 'W': //?
                    return new SqlParameter(name, SqlDbType.VarChar, -1);
                case '0':
                    return new SqlParameter(name, SqlDbType.Int);
                case 'G':
                    return new SqlParameter(name, SqlDbType.VarChar, -1);
                case 'F':
                    return new SqlParameter(name, SqlDbType.Float);
                default:
                    throw new NotSupportedException();
            }
        }
    }

    class DbfRecord
    {
        public int RecordNo { get; set; }
        public bool Deleted { get; set; }
        public object[] Fields { get; set; }
    }

    class DbfFileStream : IDisposable
    {
        private FileStream fileStream;
        private BinaryReader binaryReader;
        private Encoding textEncoding;

        public DbfHeader Header { get; }

        public IReadOnlyList<DbfFieldDescriptor> FieldDescriptors { get; }

        public IEnumerable<DbfRecord> Records
        {
            get
            {
                for (int recordNo = 0; recordNo < Header.RecordCount; recordNo++) {
                    yield return ReadRecord(recordNo, FieldDescriptors);
                }
            }
        }

        public DbfFileStream(string filename, int codepage)
        {
            if (codepage != 0)
            {
                System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
                textEncoding = Encoding.GetEncoding(codepage);
            }
            else
            {
                textEncoding = Encoding.ASCII;
            }

            fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read);
            binaryReader = new BinaryReader(fileStream);

            try {
                Header = ReadHeader(binaryReader);
            } catch (Exception e) {
                throw new Exception("Failed to read header", e);
            }

            var fieldDescriptors = new List<DbfFieldDescriptor>();
            try
            {
                int no = 0;
                while (true)
                {
                    var fieldDescriptor = ReadFieldDescriptor(binaryReader, no++);
                    if (fieldDescriptor == null)
                        break;

                    fieldDescriptors.Add(fieldDescriptor);
                }
            } catch (Exception e) {
                throw new Exception("Failed to read field descriptors", e);
            }

            // Read remainder of header

            int bytesRead = 32 + (32 * fieldDescriptors.Count) + 1;
            binaryReader.ReadBytes(Header.HeaderLength - bytesRead);

            FieldDescriptors = fieldDescriptors;
        }

        public void Dispose()
        {
            if (binaryReader != null)
            {
                binaryReader.Dispose();
                binaryReader = null;
            }

            if (fileStream != null)
            {
                fileStream.Dispose();
                fileStream = null;
            }
        }

        const string Reserved = "Reserved";

        void ReadReservedByte(string message)
        {
            ReadReservedByte(0, message);
        }

        void ReadReservedByte(byte expected = 0, string message = null)
        {
            byte reserved = binaryReader.ReadByte();
            if (reserved != expected)
                throw new NotSupportedException($"{message ?? "Reserved"} ({reserved} instead of {expected})");
        }

        void ReadReservedBytes(int count, string message)
        {
            ReadReservedBytes(count, 0, message);
        }

        void ReadReservedBytes(int count, byte expected = 0, string message = null)
        {
            byte[] reserved = binaryReader.ReadBytes(count);
            if (reserved.Length < count)
                throw new NotSupportedException(
                    $"{message ?? Reserved} ({reserved.Length} bytes read instead of {count})");

            for (int i = 0; i < count; i++)
            {
                if (reserved[i] != expected)
                    throw new NotSupportedException(
                        $"{message ?? Reserved} (byte {i} of {count} is {reserved} instead of {expected})");
            }
        }

        DbfHeader ReadHeader(BinaryReader br)
        {
            var header = new DbfHeader();

            // Version
            header.Version = br.ReadByte();

            // YYMMDD
            int yy = br.ReadByte();
            yy += (yy < 70) ? 2000 : 1900;
            int mm = br.ReadByte();
            int dd = br.ReadByte();
            header.LastUpdate = new DateTime(yy, mm, dd);

            // Records
            header.RecordCount = br.ReadInt32();

            header.HeaderLength = br.ReadInt16();

            header.RecordLength = br.ReadInt16();

            ReadReservedByte();
            ReadReservedByte();
            ReadReservedByte("Incomplete transaction");
            ReadReservedByte("Encryption");
            ReadReservedBytes(12);
            br.ReadByte(); // production mdx
            br.ReadByte(); // language driver id
            ReadReservedByte();
            ReadReservedByte();

            return header;
        }

        DbfFieldDescriptor ReadFieldDescriptor(BinaryReader br, int fdNo)
        {
            var fieldDescriptor = new DbfFieldDescriptor();
            fieldDescriptor.No = fdNo;

            try
            {
                var fieldNameBytes = new byte[11];
                fieldNameBytes[0] = br.ReadByte();
                if (fieldNameBytes[0] == 0x0D)
                    return null; // 0x0D means end of field descriptor list

                br.Read(fieldNameBytes, 1, 10);
                fieldDescriptor.Name = System.Text.Encoding.ASCII.GetString(fieldNameBytes).TrimEnd('\0');
                fieldDescriptor.TypeChar = (char) br.ReadByte();
                br.ReadByte(); // reserved
                br.ReadByte(); // reserved
                br.ReadByte(); // reserved
                br.ReadByte(); // reserved
                fieldDescriptor.Length = br.ReadByte();
                fieldDescriptor.DecimalCount = br.ReadByte();
                br.ReadBytes(2); // work area id
                br.ReadByte(); // example
                br.ReadBytes(10); // reserved
                br.ReadByte(); // production mdx

                return fieldDescriptor;
            }
            catch (Exception e)
            {
                if (string.IsNullOrWhiteSpace(fieldDescriptor.Name))
                    throw new Exception($"Failed to read field descriptor #{fdNo + 1}", e);
                else
                    throw new Exception($"Failed to read field descriptor #{fdNo + 1} ({fieldDescriptor.Name})", e);
            }
        }

        private DbfRecord ReadRecord(int recordNo, IReadOnlyList<DbfFieldDescriptor> fieldDescriptors)
        {
            try
            {
                var record = new DbfRecord();

                record.RecordNo = recordNo;

                byte status = binaryReader.ReadByte();
                if (status != 0x20 && status != 0x2A)
                    throw new NotSupportedException($"Unknown record status ({status})");
                record.Deleted = (status == 0x2A);

                record.Fields = new object[fieldDescriptors.Count];

                for (int fdNo = 0; fdNo < fieldDescriptors.Count; fdNo++)
                {
                    var fd = fieldDescriptors[fdNo];
                    try
                    {
                        record.Fields[fdNo] = ReadField(fd);
                    }
                    catch (Exception e)
                    {
                        throw new Exception($"Failed to parse column #{fd.No} ({fd.Name})", e);
                    }
                }

                return record;
            }
            catch (Exception e)
            {
                throw new Exception($"Failed to read record #{recordNo}", e);
            }
        }

        private object ReadField(DbfFieldDescriptor fd)
        {
            var data = binaryReader.ReadBytes(fd.Length);
            switch (fd.TypeChar)
            {
                case 'C':
                    return ReadFieldText(data);
                case 'I':
                    return ReadFieldInteger(data);
                case 'N':
                    return ReadFieldNumeric(data);
                case 'L':
                    return ReadFieldLogical(data);
                case 'D':
                    return ReadFieldDate(data);
                case 'M':
                    return ReadFieldMemo(data);
                case 'T':
                    return ReadFieldTime(data);
                case 'W': //?
                    return null;
                case '0'
                    : // _NullFlags?  https://stackoverflow.com/questions/30886730/adding-data-to-dbf-file-adds-column-nullflags
                    return null;
            }

            throw new NotSupportedException($"Field type ({fd.TypeChar})");
        }

        private DateTime? ReadFieldTime(byte[] data)
        {
            if (data.Length != 8)
                throw new InvalidOperationException($"Time has invalid length ({data.Length} instead of 8)");

            // "Days since Jan 1, 4713 BC"
            // JD = Days since "12h Jan 1, 4713 BC"
            // 19/09/2018 JD=2458380.5 ==> Days = 2458381
            // DateTime(19/09/2018) - DateTime.MinValue = 736955
            // "Days since Jan 1, 4713 BC" on DateTime.MinValue = 2458381 - 736955 = 1721426

            const int daysOnDateTimeMinValue = 1721426;

            int days = (data[0] << 0) | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
            int msecs = (data[4] << 0) | (data[5] << 8) | (data[6] << 16) | (data[7] << 24);

            if (days == 0 && msecs == 0)
                return null;

            return DateTime.MinValue.AddDays(days - daysOnDateTimeMinValue).AddMilliseconds(msecs);
        }

        private object ReadFieldMemo(byte[] data)
        {
            if (data.Length == 4)
            {
                if (data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0)
                    return null; //???
                if (data[0] == 0x20 && data[1] == 0x20 && data[2] == 0x20 && data[3] == 0x20)
                    return null; //???

                return "<Not implemented>";
            }

            throw new NotSupportedException($"Field type Memo with length <> 4");
        }

        private DateTime? ReadFieldDate(byte[] data)
        {
            var text = System.Text.Encoding.ASCII.GetString(data);
            if (text == "        ")
                return null;

            if (text.StartsWith("00"))
            {
                text = "20" + text.Substring(2);
            }

            return DateTime.ParseExact(text, "yyyyMMdd", null);
        }

        private bool? ReadFieldLogical(byte[] data)
        {
            if (data.Length != 1)
                throw new InvalidOperationException($"Logical has invalid length ({data.Length} instead of 1)");

            var ch = (char) data[0];
            if (ch == 'Y' || ch == 'y' || ch == 'T' || ch == 't')
                return true;

            if (ch == 'N' || ch == 'n' || ch == 'F' || ch == 'f')
                return false;

            if (ch == '?' || ch == ' ')
                return null;

            throw new NotSupportedException($"Unknown logical value ({ch})");
        }

        private decimal? ReadFieldNumeric(byte[] data)
        {
            string text = System.Text.Encoding.ASCII.GetString(data).TrimStart();

            if (text == "")
                return null;

            if (text.StartsWith('.'))
                text = '0' + text;

            var d = decimal.Parse(text, CultureInfo.InvariantCulture.NumberFormat);
            return d;
        }

        private int ReadFieldInteger(byte[] data)
        {
            if (data.Length != 4)
                throw new InvalidOperationException($"Integer has invalid length ({data.Length} instead of 4)");

            return data[0] << 0 | data[1] << 8 | data[2] << 16 | data[3] << 24;
        }

        private string ReadFieldText(byte[] data)
        {
            return textEncoding.GetString(data).TrimEnd();
        }

    }
}

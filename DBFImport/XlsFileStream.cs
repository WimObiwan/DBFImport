using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using NPOI.SS.UserModel;
using NPOI.XSSF.Extractor;
using NPOI.XSSF.UserModel;

namespace DBFImport
{
    internal class XlsFileStream : IFileStream
    {
        private XSSFWorkbook excelFile;
        private IEnumerator<XSSFRow> rowEnumerator;

        private XlsHeader header;
        public IHeader Header => header;

        private IReadOnlyList<XlsFieldDescriptor> fieldDescriptors;
        public IReadOnlyList<IFieldDescriptor> FieldDescriptors => fieldDescriptors;

        public IEnumerable<Record> Records
        {
            get
            {
                for (int recordNo = 0;; ++recordNo)
                {
                    var record = ReadRecord(recordNo++, fieldDescriptors);
                    if (record == null)
                        yield break;

                    yield return record;
                }
            }
        }

        private IEnumerator<T> CastEnumerator<T>(IEnumerator iterator)
        {
            while (iterator.MoveNext()) yield return (T)iterator.Current;
        }

        public XlsFileStream(string filename)
        {
            excelFile = new XSSFWorkbook(filename);
            if (excelFile.NumberOfSheets < 1) throw new Exception("Excel file has no sheets");

            var sheet = excelFile.GetSheetAt(0);
            rowEnumerator = CastEnumerator<XSSFRow>(sheet.GetRowEnumerator());

            try
            {
                header = ReadHeader();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to read header", e);
            }
        }

        public void Dispose()
        {
            if (excelFile != null)
            {
                //excelFile.Dispose();
                excelFile = null;
            }
        }

        private XlsHeader ReadHeader()
        {
            var dataFormatter = new DataFormatter();

            header = new XlsHeader();
            header.LastUpdate = DateTime.MinValue;
            if (!rowEnumerator.MoveNext()) throw new Exception("No header row found");

            var cellsRow1 = rowEnumerator.Current.Cells;

            if (!rowEnumerator.MoveNext()) throw new Exception("No header row 2 found");

            var cellsRow2 = rowEnumerator.Current.Cells;

            var fieldDescriptors = new List<XlsFieldDescriptor>();
            for (var cellNo = 0; cellNo < cellsRow1.Count; ++cellNo)
            {
                var fieldDescriptor = new XlsFieldDescriptor();
                fieldDescriptor.No = cellNo;
                fieldDescriptor.Name = dataFormatter.FormatCellValue(cellsRow1[cellNo]);
                
                string sqlDataType;
                if (cellNo < cellsRow2.Count)
                    sqlDataType = dataFormatter.FormatCellValue(cellsRow2[cellNo]);
                else
                    sqlDataType = null;

                if (string.IsNullOrWhiteSpace(sqlDataType))
                    sqlDataType = "nvarchar(max)";

                fieldDescriptor.SetSqlDataType(sqlDataType);
                fieldDescriptors.Add(fieldDescriptor);
            }

            this.fieldDescriptors = fieldDescriptors;
            header.FieldCount = fieldDescriptors.Count;

            return header;
        }

        private Record ReadRecord(int recordNo, IReadOnlyList<XlsFieldDescriptor> fieldDescriptors)
        {
            if (!rowEnumerator.MoveNext())
                return null;

            try
            {
                var record = new Record();
                record.RecordNo = recordNo;

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

        private object ReadField(XlsFieldDescriptor fd)
        {
            var rowCells = rowEnumerator.Current.Cells;

            var cell = rowCells.SingleOrDefault(c => c.ColumnIndex == fd.No);
            if (cell == null)
            {
                return null;
            }

            switch (cell.CellType)
            {
                case CellType.Blank:
                    return null;
                case CellType.Boolean:
                    return cell.BooleanCellValue;
                case CellType.Error:
                    return null;
                case CellType.Formula:
                    return null;
                case CellType.Numeric:
                    if (DateUtil.IsCellDateFormatted(cell))
                        return cell.DateCellValue;
                    else
                        return cell.NumericCellValue;
                case CellType.String:
                    return cell.StringCellValue;
                case CellType.Unknown:
                    return null;
                default:
                    throw new Exception($"Unknown Excel cell value type ({cell.CellType})");
            }
        }

        private class XlsHeader : IHeader
        {
            public DateTime LastUpdate { get; set; }
            public int? RecordCount => null;
            public int FieldCount { get; set; }
        }

        private class XlsFieldDescriptor : IFieldDescriptor
        {
            public int No { get; set; }
            public string Name { get; set; }

            private string sqlDbTypeText;
            public SqlDbType SqlDbType { get; private set; }
            public byte? LengthOrPrecision { get; private set; }
            public byte? Scale { get; private set; }

            public void SetSqlDataType(string sqlDataTypeString)
            {
                ParseSqlDbType(sqlDataTypeString, out SqlDbType sqlDbType, out byte? lengthOrPrecision, out byte? scale);
                AssertValidDataType(sqlDbType, lengthOrPrecision, scale);
                sqlDbTypeText = sqlDataTypeString;
                SqlDbType = sqlDbType;
                LengthOrPrecision = lengthOrPrecision;
                Scale = scale;
            }

            public string GetSqlDataType()
            {
                return sqlDbTypeText;
            }

            private static void AssertValidDataType(SqlDbType sqlDbType, byte? lengthOrPrecision, byte? scale)
            {
                switch (sqlDbType)
                {
                    case SqlDbType.BigInt:
                    case SqlDbType.Int:
                    case SqlDbType.SmallInt:
                    case SqlDbType.TinyInt:
                    case SqlDbType.Bit:
                    case SqlDbType.DateTime2:
                    case SqlDbType.DateTime:
                    case SqlDbType.Date:
                    case SqlDbType.Time:
                    case SqlDbType.Real:
                        if (lengthOrPrecision.HasValue)
                            throw new Exception($"SQL datatype {sqlDbType} must not have a length ({lengthOrPrecision})");
                        if (scale.HasValue)
                            throw new Exception($"SQL datatype {sqlDbType} must not have a precision ({scale})");
                        break;
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                    case SqlDbType.VarChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Float:
                        if (!lengthOrPrecision.HasValue)
                            throw new Exception($"SQL datatype {sqlDbType} must have a length");
                        if (scale.HasValue)
                            throw new Exception($"SQL datatype {sqlDbType} must not have a precision ({scale})");
                        break;
                    case SqlDbType.Decimal:
                        if (!lengthOrPrecision.HasValue)
                            throw new Exception($"SQL datatype {sqlDbType} must have a length");
                        // Scale is optional
                        break;
                    default:
                        throw new Exception($"SQL DB type {sqlDbType} is not supported");
                }
            }

            private static void ParseSqlDbType(string sqlDataTypeString, out SqlDbType sqlDbType, out byte? lengthOrPrecision, out byte? scale)
            {
                // INT               ^(\w+)$
                // NVARCHAR ( 64 )   ^(\w+)(?:\s*\(\s*(\d+)\s*\))?$
                // DECIMAL (15, 5)   ^(\w+)(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?$

                var match = Regex.Match(sqlDataTypeString, @"^(\w+)(?:\s*\(\s*(\d+)(?:\s*,\s*(\d+))?\s*\))?$", RegexOptions.IgnoreCase);
                if (!match.Success)
                {
                    throw new Exception($"Could not parse data type '{sqlDataTypeString}'");
                }

                string dataType = match.Groups[1].Value;
                string lengthOrPrecisionText = match.Groups[2].Success ? match.Groups[2].Value : null;
                string scaleText = match.Groups[3].Success ? match.Groups[3].Value : null;

                if (lengthOrPrecisionText != null)
                    lengthOrPrecision = byte.Parse(lengthOrPrecisionText);
                else
                    lengthOrPrecision = null;

                if (scaleText != null)
                    scale = byte.Parse(scaleText);
                else
                    scale = null;

                if (dataType.Equals("bigint", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.BigInt;
                else if (dataType.Equals("int", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Int;
                else if (dataType.Equals("smallint", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.SmallInt;
                else if (dataType.Equals("tinyint", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.TinyInt;
                else if (dataType.Equals("bit", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Bit;
                else if (dataType.Equals("float", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Float;
                else if (dataType.Equals("real", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Real;
                else if (dataType.Equals("date", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Date;
                else if (dataType.Equals("datetime", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.DateTime;
                else if (dataType.Equals("datetime2", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.DateTime2;
                else if (dataType.Equals("smalldatetime", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.SmallDateTime;
                else if (dataType.Equals("time", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Time;
                else if (dataType.Equals("decimal", StringComparison.InvariantCultureIgnoreCase)
                    || dataType.Equals("numeric", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Decimal;
                else if (dataType.Equals("char", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.Char;
                else if (dataType.Equals("nchar", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.NChar;
                else if (dataType.Equals("varchar", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.VarChar;
                else if (dataType.Equals("nvarchar", StringComparison.InvariantCultureIgnoreCase))
                    sqlDbType = SqlDbType.NVarChar;
                else
                    throw new Exception($"Data type not supported ({dataType})");

                AssertValidDataType(sqlDbType, lengthOrPrecision, scale);
            }

            public SqlParameter GetSqlParameter(string name)
            {
                switch (SqlDbType)
                {
                    case SqlDbType.BigInt:
                    case SqlDbType.Int:
                    case SqlDbType.SmallInt:
                    case SqlDbType.TinyInt:
                    case SqlDbType.Bit:
                    case SqlDbType.DateTime2:
                    case SqlDbType.DateTime:
                    case SqlDbType.Date:
                    case SqlDbType.Time:
                    case SqlDbType.Real:
                        return new SqlParameter(name, SqlDbType);
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                    case SqlDbType.VarChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Float:
                        return new SqlParameter(name, SqlDbType, LengthOrPrecision.GetValueOrDefault(0));
                    case SqlDbType.Decimal:
                        var par = new SqlParameter(name, SqlDbType.Decimal);
                        par.Precision = LengthOrPrecision.GetValueOrDefault(0);
                        par.Scale = Scale.GetValueOrDefault(0);
                        return par;
                    default:
                        throw new Exception($"SQL DB type {SqlDbType} is not supported");
                }
            }

            //public bool IsDate =>
            //    SqlDbType == SqlDbType.DateTime2 ||
            //    SqlDbType == SqlDbType.DateTime ||
            //    SqlDbType == SqlDbType.Date ||
            //    SqlDbType == SqlDbType.SmallDateTime;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace DBFImport
{
    static class FileStreamFactory
    {
        public static IFileStream Create(string filename, int codepage)
        {
            string ext = Path.GetExtension(filename);
            if (ext.Equals(".dbf", StringComparison.InvariantCultureIgnoreCase)) {
                return new DbfFileStream(filename, codepage);
            } else if (ext.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase)) {
                return new XlsFileStream(filename);
            } else {
                throw new NotSupportedException($"Unsupported file extension '{ext}'");
            }
        }
    }
}

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
        static IFileStream Create(string filename, int codepage)
        {
            string ext = Path.GetExtension(filename);
            if (ext.Equals(".dbf", StringComparison.InvariantCultureIgnoreCase)) {
                return new DbfFileStream(filename, codepage);
            } else {
                throw new NotSupportedException($"Unsupported file extension '{ext}'");
            }
        }
    }
}

using System.Collections.Generic;
using System.Data;

using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData.Utilities.Interfaces
{
    public interface ISqlUtility<T> where T : IDbConnection
    {
        List<ColumnMetadata> SelectColumnMetadata(T connection, string tableName);
    }
}

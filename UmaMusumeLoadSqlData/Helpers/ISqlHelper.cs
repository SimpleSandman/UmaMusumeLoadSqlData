using System.Collections.Generic;
using System.Data;

using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData.Helpers
{
    public interface ISqlHelper<T> where T : IDbConnection
    {
        List<ColumnMetadata> SelectColumnMetadata(T connection, string tableName);
    }
}

using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace UmaMusumeLoadSqlData.Utilities.Interfaces
{
    public interface ISqlDestination<T> where T : IDbConnection
    {
        List<string> SelectTableNames(T connection);
        Task<bool> TryBulkInsertDataTableAsync(T connection, string tableName, DataTable dataTable, bool isFirstAttempt = true);
    }
}

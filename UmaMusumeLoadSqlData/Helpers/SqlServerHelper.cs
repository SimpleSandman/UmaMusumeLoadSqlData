using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData.Helpers
{
    public class SqlServerHelper : ISqlHelper<SqlConnection>
    {
        public List<ColumnMetadata> SelectColumnMetadata(SqlConnection connection, string tableName)
        {
            List<ColumnMetadata> result = new List<ColumnMetadata>();

            string commandText =
                @$"SELECT c.name AS ColumnName
                     , TYPE_NAME(c.system_type_id) AS DataType
                     , c.is_nullable AS IsNullable
                FROM sys.tables t
                    JOIN sys.schemas s
                        ON t.schema_id = s.schema_id
                    JOIN sys.columns c
                        ON t.object_id = c.object_id
                WHERE t.name = '{tableName}'
                    AND s.schema_id = SCHEMA_ID('RawData')
                ORDER BY t.name
                       , c.column_id";

            using (SqlCommand command = new SqlCommand(commandText, connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(new ColumnMetadata
                        {
                            ColumnName = reader.GetString(0),
                            ColumnDataType = reader.GetString(1),
                            IsNullable = reader.GetBoolean(2)
                        });
                    }
                }
            }

            return result;
        }

        public bool TryBulkInsertDataTable(string tableName, DataTable dataTable, SqlConnection connection, bool isFirstAttempt = true)
        {
            try
            {
                SqlBulkCopy bulkCopy = new SqlBulkCopy(connection,
                    SqlBulkCopyOptions.TableLock
                        | SqlBulkCopyOptions.FireTriggers
                        | SqlBulkCopyOptions.UseInternalTransaction,
                    null);
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.WriteToServer(dataTable);

                return true;
            }
            catch (Exception ex)
            {
                if (!isFirstAttempt)
                {
                    Console.WriteLine($"\nERROR: Could not bulk insert for the table \"{tableName}\"");
                    Console.WriteLine(ex.Message);

                    // Reference: https://sqlbulkcopy-tutorial.net/columnmapping-does-not-match
                    if (!ex.Message.Contains("The given ColumnMapping does not match up with any column in the source or destination"))
                    {
                        Console.WriteLine(ex.StackTrace);
                    }
                }
            }

            return false;
        }

        public List<string> SelectSingleColumn(string commandText, SqlConnection connection)
        {
            List<string> result = new List<string>();

            using (SqlCommand command = new SqlCommand(commandText, connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString(0));
                    }
                }
            }

            return result;
        }
    }
}

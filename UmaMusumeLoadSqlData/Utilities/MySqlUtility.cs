using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

using MySqlConnector;

using UmaMusumeLoadSqlData.Utilities.Interfaces;
using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData.Utilities
{
    public class MySqlUtility : ISqlUtility<MySqlConnection>, ISqlDestination<MySqlConnection>
    {
        #region ISqlUtility Methods
        public List<ColumnMetadata> SelectColumnMetadata(MySqlConnection connection, string tableName)
        {
            List<ColumnMetadata> result = new List<ColumnMetadata>();

            // TODO: Store database name (table_schema) somewhere else
            string commandText =
                @$"SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE
                FROM information_schema.COLUMNS
                WHERE TABLE_NAME = '{tableName}'
                    AND TABLE_SCHEMA = '{connection.Database}'
                ORDER BY TABLE_NAME, ORDINAL_POSITION;";

            using (MySqlCommand command = new MySqlCommand(commandText, connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
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
        #endregion

        #region ISqlDestination Methods
        public List<string> SelectTableNames(MySqlConnection connection)
        {
            List<string> result = new List<string>();

            // TODO: Store database name (table_schema) somewhere else
            using (MySqlCommand command = new MySqlCommand($"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{connection.Database}'", connection))
            {
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(reader.GetString(0));
                    }
                }
            }

            return result;
        }

        public async Task<bool> TryBulkInsertDataTableAsync(MySqlConnection connection, string tableName, DataTable dataTable, bool isFirstAttempt = true)
        {
            try
            {
                MySqlBulkCopy bulkCopy = new MySqlBulkCopy(connection)
                {
                    DestinationTableName = tableName
                };

                await bulkCopy.WriteToServerAsync(dataTable);
                Console.WriteLine($"Successfully loaded {tableName}");

                return true;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("To use MySqlBulkLoader.Local=true, set AllowLoadLocalInfile=true in the connection string"))
                {
                    Console.WriteLine($"\nFATAL ERROR: MySql connection string doesn't contain \"AllowLoadLocalInfile = true\".");
                    Console.WriteLine("This is required for BULK COPY inserts into a table.");

                    Program.CloseProgram();
                }

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
        #endregion
    }
}

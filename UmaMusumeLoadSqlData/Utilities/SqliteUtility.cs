using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;

using UmaMusumeLoadSqlData.Models;
using UmaMusumeLoadSqlData.Utilities.Interfaces;

namespace UmaMusumeLoadSqlData.Utilities
{
    public class SqliteUtility : ISqlUtility<SQLiteConnection>
    {
        #region ISqlUtility Method
        public List<ColumnMetadata> SelectColumnMetadata(SQLiteConnection connection, string tableName)
        {
            List<ColumnMetadata> result = new List<ColumnMetadata>();

            using (SQLiteCommand command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT name, type, [notnull] FROM PRAGMA_TABLE_INFO('{tableName}');";

                using (SQLiteDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        result.Add(new ColumnMetadata
                        {
                            ColumnName = reader.GetString(0),
                            ColumnDataType = reader.GetString(1),
                            IsNullable = !reader.GetBoolean(2) // reverse null return
                        });

                    }
                }
            }

            return result;
        }
        #endregion

        public void LoadSqliteDataTables(SQLiteConnection connection, List<SqliteMasterRecord> sqliteTableNames, List<DataTable> sqliteDataTables)
        {
            using (SQLiteCommand tableDataCommand = connection.CreateCommand())
            {
                foreach (string sqliteTableName in sqliteTableNames.Select(n => n.TableName))
                {
                    tableDataCommand.CommandText = $"SELECT * FROM [{sqliteTableName}]";

                    using (SQLiteDataReader reader = tableDataCommand.ExecuteReader())
                    {
                        DataTable dt = new DataTable();
                        dt.Load(reader);
                        sqliteDataTables.Add(dt);
                    }
                }
            }
        }

        public void SqliteTableNames(SQLiteConnection connection, List<SqliteMasterRecord> sqliteTableNames)
        {
            using (SQLiteCommand tableNameCommand = connection.CreateCommand())
            {
                tableNameCommand.CommandText = "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'table' ORDER BY tbl_name";

                using (SQLiteDataReader reader = tableNameCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);
                        if (tableName != "sqlite_stat1")
                        {
                            sqliteTableNames.Add(new SqliteMasterRecord 
                            { 
                                TableName = tableName, 
                                SqlScript = reader.GetString(1) 
                            });
                        }
                    }
                }
            }
        }

        public void SqliteIndexScript(SQLiteConnection connection, List<SqliteMasterRecord> sqliteIndexNames)
        {
            using (SQLiteCommand indexNameCommand = connection.CreateCommand())
            {
                indexNameCommand.CommandText = "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY tbl_name";

                using (SQLiteDataReader reader = indexNameCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);
                        if (tableName != "sqlite_stat1")
                        {
                            sqliteIndexNames.Add(new SqliteMasterRecord
                            {
                                TableName = tableName,
                                SqlScript = reader.GetString(1)
                            });
                        }
                    }
                }
            }
        }
    }
}

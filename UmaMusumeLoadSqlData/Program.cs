using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace UmaMusumeLoadSqlData
{
    public class Program
    {
        private static readonly List<string> _sqliteTableNames = new List<string>();
        private static readonly List<DataTable> _sqliteDataTables = new List<DataTable>();

        static void Main()
        {
            try
            {
                /* Verify master.mdb exists */
                string masterDbFilepath = @$"{Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)}\AppData\LocalLow\Cygames\umamusume\master\master.mdb";

                if (File.Exists(masterDbFilepath))
                {
                    Console.WriteLine("Found master.mdb");
                }
                else
                {
                    Console.WriteLine("Cannot find master.mdb");
                    CloseProgram();
                }

                /* Pull table info from the SQLite master.mdb */
                using (SQLiteConnection connection = new SQLiteConnection($"Data Source={masterDbFilepath}"))
                {
                    connection.Open();

                    // Get raw data table names needed to loop
                    SqliteTableNames(connection);

                    // Loop through each table and pull all available data
                    LoadSqliteDataTables(connection);
                }

                /* Push new info into UmaMusume MSSQL database */
                // TODO: Store connection string elsewhere
                using (SqlConnection connection = new SqlConnection("Server=.;Database=UmaMusume;Integrated Security=SSPI;"))
                {
                    connection.Open();

                    // Check if there are any new SQLite tables that need to be created
                    List<string> sqlTableNames = SelectColumn("SELECT name FROM sys.tables", connection);
                    foreach (string name in sqlTableNames)
                    {
                        _sqliteTableNames.Remove(name);
                    }

                    // Alert the owner of any new tables
                    if (_sqliteTableNames.Count > 0)
                    {
                        Console.WriteLine($"WARNING: {_sqliteTableNames.Count} new table(s) found from the master.mdb file");
                        foreach (string tableName in _sqliteTableNames)
                        {
                            Console.WriteLine($"\"{tableName}\"");
                        }

                        // Remove sqliteDataTables that don't exist in MSSQL yet
                        _sqliteDataTables.RemoveAll(t => _sqliteTableNames.Contains(t.TableName));
                    }

                    foreach (DataTable sqliteDataTable in _sqliteDataTables.OrderBy(t => t.TableName))
                    {
                        // Start wtih a clean slate
                        using (SqlCommand truncateCommand = new SqlCommand($"TRUNCATE TABLE [RawData].[{sqliteDataTable.TableName}]", connection))
                        {
                            truncateCommand.ExecuteNonQuery();
                        }

                        // Load DataTable into MSSQL
                        BulkInsertDataTable($"RawData.{sqliteDataTable.TableName}", sqliteDataTable, connection);
                    }

                    Console.WriteLine("Raw table reload successful!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                CloseProgram();
            }
        }

        #region Private Methods
        private static void LoadSqliteDataTables(SQLiteConnection connection)
        {
            using (SQLiteCommand tableDataCommand = connection.CreateCommand())
            {
                foreach (string sqliteTableName in _sqliteTableNames)
                {
                    tableDataCommand.CommandText = $"SELECT * FROM [{sqliteTableName}]";

                    using (SQLiteDataReader reader = tableDataCommand.ExecuteReader())
                    {
                        DataTable dt = new DataTable();
                        dt.Load(reader);
                        _sqliteDataTables.Add(dt);
                    }
                }
            }
        }

        private static void SqliteTableNames(SQLiteConnection connection)
        {
            using (SQLiteCommand tableNameCommand = connection.CreateCommand())
            {
                tableNameCommand.CommandText = "SELECT tbl_name FROM sqlite_master WHERE type = 'table' ORDER BY tbl_name";

                using (SQLiteDataReader reader = tableNameCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader.GetString(0);
                        if (tableName != "sqlite_stat1")
                        {
                            _sqliteTableNames.Add(tableName);
                        }
                    }
                }
            }
        }

        private static bool BulkInsertDataTable(string tableName, DataTable dataTable, SqlConnection connection)
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
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        private static List<string> SelectColumn(string commandText, SqlConnection connection)
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

        private static void CloseProgram()
        {
            Console.WriteLine("\nPress any key to close this program...");
            Console.ReadKey();
            Environment.Exit(0);
        }
        #endregion
    }
}

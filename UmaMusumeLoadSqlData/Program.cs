using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;

using UmaMusumeLoadSqlData.Helpers;
using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData
{
    public class Program
    {
        private static readonly List<SqliteMasterRecord> _sqliteTableNames = new List<SqliteMasterRecord>();
        private static readonly List<DataTable> _sqliteDataTables = new List<DataTable>();
        private static readonly SqliteHelper _sqliteHelper = new SqliteHelper();
        private static readonly SqlServerHelper _sqlServerHelper = new SqlServerHelper();

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
                    _sqliteHelper.SqliteTableNames(connection, _sqliteTableNames);

                    // Loop through each table and pull all available data
                    _sqliteHelper.LoadSqliteDataTables(connection, _sqliteTableNames, _sqliteDataTables);
                }

                /* Push new info into UmaMusume MSSQL database */
                // TODO: Store connection string elsewhere
                using (SqlConnection sqlServerConnection = new SqlConnection("Server=.;Database=UmaMusume;Integrated Security=SSPI;"))
                {
                    sqlServerConnection.Open();

                    // Check if there are any new SQLite tables that need to be created
                    List<string> sqlTableNames = _sqlServerHelper.SelectSingleColumn("SELECT name FROM sys.tables", sqlServerConnection);
                    foreach (string name in sqlTableNames)
                    {
                        _sqliteTableNames.RemoveAll(t => t.TableName == name);
                    }

                    // Alert the owner of any new tables
                    if (_sqliteTableNames.Count > 0)
                    {
                        Console.WriteLine($"WARNING: {_sqliteTableNames.Count} new table(s) found from the master.mdb file");
                        foreach (string tableName in _sqliteTableNames.Select(n => n.TableName))
                        {
                            Console.WriteLine($"\"{tableName}\"");
                        }

                        // Remove sqliteDataTables that don't exist in MSSQL yet
                        _sqliteDataTables.RemoveAll(t => _sqliteTableNames.Any(n => n.TableName == t.TableName));
                    }

                    bool hadBulkInsertError = false;

                    foreach (DataTable sqliteDataTable in _sqliteDataTables.OrderBy(t => t.TableName))
                    {
                        // Start wtih a clean slate
                        using (SqlCommand truncateCommand = new SqlCommand($"TRUNCATE TABLE [RawData].[{sqliteDataTable.TableName}]", sqlServerConnection))
                        {
                            truncateCommand.ExecuteNonQuery();
                        }

                        // Load DataTable into MSSQL
                        // If error occurs, display possible issues and possibly remedy them
                        if (!_sqlServerHelper.TryBulkInsertDataTable($"RawData.{sqliteDataTable.TableName}", sqliteDataTable, sqlServerConnection))
                        {
                            using (SQLiteConnection sqliteDebugConnection = new SQLiteConnection($"Data Source={masterDbFilepath}"))
                            {
                                sqliteDebugConnection.Open();

                                List<ColumnMetadata> sqliteColumns = _sqliteHelper.SelectColumnMetadata(sqliteDebugConnection, sqliteDataTable.TableName);
                                List<ColumnMetadata> sqlServerColumns = _sqlServerHelper.SelectColumnMetadata(sqlServerConnection, sqliteDataTable.TableName);

                                // Add missing columns
                                foreach (ColumnMetadata missingColumn in sqliteColumns.Where(lite => !sqlServerColumns.Exists(s => s.ColumnName == lite.ColumnName)))
                                {
                                    string isNullable = "NOT NULL";
                                    if (missingColumn.IsNullable)
                                    {
                                        isNullable = "NULL";
                                    }

                                    string addColumn = "";
                                    if (missingColumn.ColumnDataType == "INTEGER")
                                    {
                                        // Reference: https://stackoverflow.com/a/7337945/2113548 (similar issue for SQLite's TEXT)
                                        addColumn = $"ALTER TABLE [RawData].[{sqliteDataTable.TableName}] ADD [{missingColumn.ColumnName}] BIGINT {isNullable}";
                                    }
                                    else if (missingColumn.ColumnDataType == "TEXT")
                                    {
                                        addColumn = $"ALTER TABLE [RawData].[{sqliteDataTable.TableName}] ADD [{missingColumn.ColumnName}] NVARCHAR(4000) {isNullable}";
                                    }
                                    else
                                    {
                                        Console.WriteLine($"ERROR: Unable to handle SQLite datatype: {missingColumn.ColumnDataType}");
                                        CloseProgram();
                                    }

                                    // Try to add the missing columns
                                    try
                                    {
                                        using (SqlCommand addColumnCommand = new SqlCommand(addColumn, sqlServerConnection))
                                        {
                                            addColumnCommand.ExecuteNonQuery();
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"\nERROR: {ex.Message}");

                                        if (string.IsNullOrEmpty(addColumn))
                                        {
                                            Console.WriteLine($"SQL Command: {addColumn}");
                                        }

                                        hadBulkInsertError = true;
                                    }
                                }
                            }

                            // Try to insert again with the newly added columns
                            if (!_sqlServerHelper.TryBulkInsertDataTable($"RawData.{sqliteDataTable.TableName}", sqliteDataTable, sqlServerConnection, false))
                            {
                                hadBulkInsertError = true;
                            }
                        }
                    }

                    // Provide special output per error
                    if (hadBulkInsertError)
                    {
                        Console.WriteLine("\nRaw table reload successful, but has bulk insert errors.");
                        Console.WriteLine("Please read the error messages as these tables are currently empty.");
                    }
                    else
                    {
                        Console.WriteLine("\nRaw table reload successful!");
                    }
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
        private static void CloseProgram()
        {
            Console.WriteLine("\nPress any key to close this program...");
            Console.ReadKey();
            Environment.Exit(0);
        }
        #endregion
    }
}

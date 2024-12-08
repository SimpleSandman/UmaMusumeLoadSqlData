using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using MySqlConnector;

using UmaMusumeLoadSqlData.Models;
using UmaMusumeLoadSqlData.Utilities;

namespace UmaMusumeLoadSqlData
{
    public class Program
    {
        #region Private Class Variables
        private static readonly SqliteUtility _sqliteUtility = new();
        private static readonly MySqlUtility _mySqlUtility = new();
        private static bool _hadBulkInsertError = false;
        #endregion

        #region Private Const Variables
        private const string SOURCE_MASTER_FILENAME = "master/master.mdb";
        private const string SOURCE_META_FILENAME = "meta";
        #endregion

        #region Command Line Arg Class Variables
        public static string AspNetCoreEnvironment { get; private set; }
        private static string _repoName;
        private static string _branchName;
        private static string _mySqlConnectionString;
        public static bool IsVerbose { get; private set; } = false;
        #endregion

        static void Main(string[] args)
        {
            AspNetCoreEnvironment = args[0];
            _repoName = args[1];
            _branchName = args[2];
            _mySqlConnectionString = args[3];

            if (args.Length >= 5 && args[4].ToLower() == "verbose")
            {
                IsVerbose = true;
            }

            MainAsync().GetAwaiter().GetResult();
        }

        #region Actual Main Method
        static async Task MainAsync()
        {
            Console.WriteLine($">>> Now running in \"{AspNetCoreEnvironment}\" <<<\n");

            try
            {
                string masterDbDestinationFilepath = @$"{Environment.CurrentDirectory}\master.mdb";
                string metaDbDestinationFilepath = @$"{Environment.CurrentDirectory}\meta";

                if (AspNetCoreEnvironment == "Development" && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    masterDbDestinationFilepath 
                        = @$"{Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)}\AppData\LocalLow\Cygames\umamusume\master\master.mdb";
                    metaDbDestinationFilepath
                        = @$"{Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)}\AppData\LocalLow\Cygames\umamusume\meta";
                }
                else
                {
                    // Download meta and master.mdb files from remote GitHub repo
                    await GithubUtility.DownloadRemoteFileAsync(_repoName, _branchName, SOURCE_MASTER_FILENAME, masterDbDestinationFilepath);
                    await GithubUtility.DownloadRemoteFileAsync(_repoName, _branchName, SOURCE_META_FILENAME, metaDbDestinationFilepath);
                }

                /* Verify master.mdb exists */
                if (File.Exists(masterDbDestinationFilepath) && File.Exists(metaDbDestinationFilepath))
                {
                    Console.WriteLine("SUCCESS: Found meta and master.mdb files");
                }
                else
                {
                    Console.WriteLine("FATAL ERROR: Cannot find meta and master.mdb files");
                    CloseProgram();
                }

                await LoadDatabaseData(masterDbDestinationFilepath, SOURCE_MASTER_FILENAME);
                await LoadDatabaseData(metaDbDestinationFilepath, SOURCE_META_FILENAME);

                // Provide special output per error
                if (_hadBulkInsertError)
                {
                    Console.WriteLine("\nWARNING: Table reload successful, but has bulk insert errors.");
                }
                else
                {
                    Console.WriteLine("\nSUCCESS: Table reload successful!");
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
        #endregion

        #region Private Methods
        private static async Task LoadDatabaseData(string dbFilepath, string sourceFilename)
        {
            List<SqliteMasterRecord> sqliteMasterTableNames = [];
            List<SqliteMasterRecord> sqliteMasterIndexNames = [];
            List<DataTable> sqliteMasterDataTables = [];

            /* Pull table info from the SQLite master.mdb */
            using (SQLiteConnection connection = new($"Data Source={dbFilepath}"))
            {
                connection.Open();

                // Get raw data table names needed to loop
                SqliteUtility.SqliteTableNames(connection, sqliteMasterTableNames);

                // Get raw data index names needed to loop
                SqliteUtility.SqliteIndexScript(connection, sqliteMasterIndexNames);

                // Loop through each table and pull all available data
                SqliteUtility.LoadSqliteDataTables(connection, sqliteMasterTableNames, sqliteMasterDataTables);
            }

            /* Import data from SQLite into the provided database(s) below */
            if (!string.IsNullOrEmpty(_mySqlConnectionString) && _mySqlConnectionString != "N/A")
            {
                await SqlDestinationAsync<MySqlConnection, MySqlCommand>(sourceFilename, _mySqlConnectionString,
                    sqliteMasterTableNames, sqliteMasterIndexNames, sqliteMasterDataTables).ConfigureAwait(false);
            }

            // Memory clean up
            sqliteMasterTableNames.Clear();
            sqliteMasterIndexNames.Clear();
            sqliteMasterDataTables.Clear();
        }

        private static async Task SqlDestinationAsync<T, U>(string sourceFilename, string connectionString, 
            List<SqliteMasterRecord> sqliteMasterTableNames, List<SqliteMasterRecord> sqliteMasterIndexNames, List<DataTable> sqliteMasterDataTables) 
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            try
            {
                string tableSchema = ""; // TODO: Save for possible future database that requires a schema
                Console.WriteLine($"\nAttempting to load \"{sourceFilename}\" table data into a MySQL/MariaDB database...\n");

                /* Push new info into destination database */
                using T destinationConnection = new();
                destinationConnection.ConnectionString = connectionString;
                destinationConnection.Open();

                // Check if there are any new SQLite tables that need to be created
                string tablePrefix = sourceFilename == SOURCE_META_FILENAME 
                    ? "meta_" 
                    : "";

                List<string> sqlTableNames = SelectTableNames(destinationConnection);
                foreach (string name in sqlTableNames)
                {
                    sqliteMasterTableNames.RemoveAll(t => tablePrefix + t.TableName == name);
                }

                // Alert the owner of any new tables with their indexes
                if (sqliteMasterTableNames.Count > 0)
                {
                    Console.WriteLine($"WARNING: {sqliteMasterTableNames.Count} new table(s) found from the \"{sourceFilename}\" file...");
                    Console.WriteLine("-----------------------------------------------------------");

                    // NOTE: Local dev strings used and displayed
                    string scaffoldContent = "Scaffold-DbContext 'User Id=root;Password=sa;Host=localhost;Database=umamusume;Character Set=utf8mb4' "
                            + $"Pomelo.EntityFrameworkCore.MySql -OutputDir Models -ContextDir Context -T ";

                    foreach (SqliteMasterRecord table in sqliteMasterTableNames)
                    {
                        Console.WriteLine();

                        Console.WriteLine($"{table.SqlScript};");
                        foreach (SqliteMasterRecord index in sqliteMasterIndexNames.Where(i => i.TableName == table.TableName))
                        {
                            Console.WriteLine($"{index.SqlScript};");
                        }

                        scaffoldContent += $"{table.TableName},";
                    }

                    // Script the model creator needed for the API using Scaffold-DbContext
                    Console.WriteLine("\n-----------------------------------------------------------\n");
                    Console.WriteLine(scaffoldContent[0..^1]);
                    Console.WriteLine("\n-----------------------------------------------------------\n");

                    // Remove sqliteDataTables that don't exist in the destination table yet
                    sqliteMasterDataTables.RemoveAll(t => sqliteMasterTableNames.Any(n => n.TableName == t.TableName));
                }

                foreach (DataTable sqliteDataTable in sqliteMasterDataTables.OrderBy(t => t.TableName))
                {
                    string tableName = $"{tableSchema}{tablePrefix}{sqliteDataTable.TableName}";

                    // Start with a clean slate
                    using (U truncateCommand = new())
                    {
                        truncateCommand.CommandText = $"TRUNCATE TABLE {tableName}";
                        truncateCommand.Connection = destinationConnection;
                        truncateCommand.ExecuteNonQuery();
                    }

                    // Load DataTable into the destination database
                    if (!await TryBulkInsertDataTableAsync(destinationConnection, tableName, sqliteDataTable))
                    {
                        // Look at the SQLite table and find the missing columns for the destination table
                        _hadBulkInsertError = AreMissingColumnsResolved<T, U>(sourceFilename, destinationConnection, sqliteDataTable, tableSchema);

                        // Try to insert again with the newly added columns
                        if (!await TryBulkInsertDataTableAsync(destinationConnection, tableName, sqliteDataTable, false))
                        {
                            _hadBulkInsertError = true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static bool AreMissingColumnsResolved<T, U>(string dbFilepath, T destinationConnection, DataTable sqliteDataTable, string tableSchema)
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            try
            {
                using SQLiteConnection sqliteDebugConnection = new($"Data Source={dbFilepath}");
                sqliteDebugConnection.Open();

                List<ColumnMetadata> sqliteColumns = SelectColumnMetadata(sqliteDebugConnection, sqliteDataTable.TableName);
                List<ColumnMetadata> destinationColumns = SelectColumnMetadata(destinationConnection, sqliteDataTable.TableName);

                if (destinationColumns == null)
                {
                    Console.WriteLine($"WARNING: Missing destination columns for table, \"{sqliteDataTable.TableName}\"");
                    return false;
                }

                // Add missing columns
                foreach (ColumnMetadata missingColumn in sqliteColumns.Where(lite => !destinationColumns.Exists(s => s.ColumnName == lite.ColumnName)))
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
                        addColumn = $"ALTER TABLE {tableSchema}{sqliteDataTable.TableName} ADD {missingColumn.ColumnName} INT {isNullable}";
                    }
                    else if (missingColumn.ColumnDataType == "TEXT")
                    {
                        if (typeof(U) == typeof(MySqlCommand))
                        {
                            // NOTE: Make sure MySQL database is set to "utf8mb4" character set
                            addColumn = $"ALTER TABLE {tableSchema}{sqliteDataTable.TableName} ADD {missingColumn.ColumnName} TEXT {isNullable}";
                        }
                        else
                        {
                            addColumn = $"ALTER TABLE {tableSchema}{sqliteDataTable.TableName} ADD {missingColumn.ColumnName} NVARCHAR(4000) {isNullable}";
                        }
                    }
                    else
                    {
                        Console.WriteLine($"ERROR: Unable to handle SQLite datatype: {missingColumn.ColumnDataType}");
                        CloseProgram();
                    }

                    // Try to add the missing columns
                    try
                    {
                        using (U addColumnCommand = new())
                        {
                            addColumnCommand.CommandText = addColumn;
                            addColumnCommand.Connection = destinationConnection;
                            addColumnCommand.ExecuteNonQuery();
                        }

                        Console.WriteLine($"\nSuccessfully added missing column with script: \"{addColumn}\"");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"\nERROR: {ex.Message}");

                        if (string.IsNullOrEmpty(addColumn))
                        {
                            Console.WriteLine($"SQL Command: {addColumn}");
                        }

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        private static List<string> SelectTableNames<T>(T connection) where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(MySqlConnection))
                {
                    return _mySqlUtility.SelectTableNames(connection as MySqlConnection);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return null;
        }

        private static List<ColumnMetadata> SelectColumnMetadata<T>(T connection, string tableName) where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(MySqlConnection))
                {
                    return _mySqlUtility.SelectColumnMetadata(connection as MySqlConnection, tableName);
                }
                else if (typeof(T) == typeof(SQLiteConnection))
                {
                    return _sqliteUtility.SelectColumnMetadata(connection as SQLiteConnection, tableName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return null;
        }

        private static async Task<bool> TryBulkInsertDataTableAsync<T>(T connection, string tableName, DataTable sourceDataTable, bool isFirstAttempt = true)
            where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(MySqlConnection))
                {
                    return await _mySqlUtility.TryBulkInsertDataTableAsync(connection as MySqlConnection, tableName, sourceDataTable, isFirstAttempt);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }
        #endregion

        #region Public Methods
        public static void CloseProgram(int exitCode = 0)
        {
            if (AspNetCoreEnvironment == "Development" && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Console.WriteLine("\nPress any key to close this program...");
                Console.ReadKey();
            }
            else
            {
                Console.WriteLine("\nProgram finished executing.");
                Console.WriteLine("Exiting now...");
            }
            
            Environment.Exit(exitCode);
        }
        #endregion
    }
}

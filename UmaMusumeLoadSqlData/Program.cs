using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using CsvHelper;
using CsvHelper.Configuration;

using MySqlConnector;

using UmaMusumeLoadSqlData.Utilities;
using UmaMusumeLoadSqlData.Models;

namespace UmaMusumeLoadSqlData
{
    public class Program
    {
        private static readonly List<SqliteMasterRecord> _sqliteTableNames = new List<SqliteMasterRecord>();
        private static readonly List<DataTable> _sqliteDataTables = new List<DataTable>();
        private static readonly SqliteUtility _sqliteUtility = new SqliteUtility();
        private static readonly SqlServerUtility _sqlServerUtility = new SqlServerUtility();
        private static readonly MySqlUtility _mySqlUtility = new MySqlUtility();
        private static bool _hadBulkInsertError = false;

        // Command-line arguments
        private static string _aspNetCoreEnvironment;
        private static string _repoName;
        private static string _branchName;
        private static string _mySqlConnectionString;
        private static string _sqlServerConnectionString;

        private static string _masterDbFilepath = @$"{Environment.CurrentDirectory}\master.mdb";
        
        static void Main(string[] args)
        {
            _aspNetCoreEnvironment = args[0];
            _repoName = args[1];
            _branchName = args[2];
            _mySqlConnectionString = args[3];
            _sqlServerConnectionString = args[4];

            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            Console.WriteLine($">>> Now running in \"{_aspNetCoreEnvironment}\" <<<\n");

            try
            {
                if (_aspNetCoreEnvironment == "Development" && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    _masterDbFilepath 
                        = @$"{Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)}\AppData\LocalLow\Cygames\umamusume\master\master.mdb";
                }
                else
                {
                    // Download master.mdb file from remote GitHub repo
                    GithubUtility.DownloadRemoteFile(_repoName, _branchName, "master.mdb", _masterDbFilepath);
                }

                /* Verify master.mdb exists */
                if (File.Exists(_masterDbFilepath))
                {
                    Console.WriteLine("SUCCESS: Found master.mdb");
                }
                else
                {
                    Console.WriteLine("FATAL ERROR: Cannot find master.mdb");
                    CloseProgram();
                }

                /* Pull table info from the SQLite master.mdb */
                using (SQLiteConnection connection = new SQLiteConnection($"Data Source={_masterDbFilepath}"))
                {
                    connection.Open();

                    // Get raw data table names needed to loop
                    _sqliteUtility.SqliteTableNames(connection, _sqliteTableNames);

                    // Loop through each table and pull all available data
                    _sqliteUtility.LoadSqliteDataTables(connection, _sqliteTableNames, _sqliteDataTables);
                }

                /* Import data from SQLite into the provided database(s) below */
                if (!string.IsNullOrEmpty(_mySqlConnectionString) && _mySqlConnectionString != "N/A")
                {
                    await SqlDestination<MySqlConnection, MySqlCommand>(_mySqlConnectionString).ConfigureAwait(false);
                    await LoadEnglishTranslationsAsync<MySqlConnection, MySqlCommand>(_mySqlConnectionString).ConfigureAwait(false);
                }

                if (!string.IsNullOrEmpty(_sqlServerConnectionString) && _sqlServerConnectionString != "N/A")
                {
                    await SqlDestination<SqlConnection, SqlCommand>(_sqlServerConnectionString).ConfigureAwait(false);
                    await LoadEnglishTranslationsAsync<SqlConnection, SqlCommand>(_sqlServerConnectionString).ConfigureAwait(false);
                }

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

        #region Private Methods
        private static async Task SqlDestination<T, U>(string connectionString) 
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            try
            {
                // MSSQL uses a separate schema name than the database
                string tableSchema = "RawData.";
                if (typeof(T) == typeof(MySqlConnection))
                {
                    tableSchema = "";
                    Console.WriteLine("\nAttempting to load table data into a MySQL/MariaDB database...\n");
                }
                else
                {
                    Console.WriteLine("\nAttempting to load table data into a SQL Server database...\n");
                }

                /* Push new info into destination database */
                using (T destinationConnection = new T())
                {
                    destinationConnection.ConnectionString = connectionString;
                    destinationConnection.Open();

                    // Check if there are any new SQLite tables that need to be created
                    List<string> sqlTableNames = SelectTableNames(destinationConnection);
                    foreach (string name in sqlTableNames)
                    {
                        _sqliteTableNames.RemoveAll(t => t.TableName == name);
                    }

                    // Alert the owner of any new tables
                    if (_sqliteTableNames.Count > 0)
                    {
                        Console.WriteLine($"WARNING: {_sqliteTableNames.Count} new table(s) found from the master.mdb file");
                        Console.WriteLine("-------------------------------------------------------------------------------");
                        
                        foreach (SqliteMasterRecord table in _sqliteTableNames)
                        {
                            Console.WriteLine(table.SqlScript);
                        }

                        Console.WriteLine("-------------------------------------------------------------------------------\n");

                        // Remove sqliteDataTables that don't exist in the destination table yet
                        _sqliteDataTables.RemoveAll(t => _sqliteTableNames.Any(n => n.TableName == t.TableName));
                    }

                    foreach (DataTable sqliteDataTable in _sqliteDataTables.OrderBy(t => t.TableName))
                    {
                        // Start with a clean slate
                        using (U truncateCommand = new U())
                        {
                            truncateCommand.CommandText = $"TRUNCATE TABLE {tableSchema}{sqliteDataTable.TableName}";
                            truncateCommand.Connection = destinationConnection;
                            truncateCommand.ExecuteNonQuery();
                        }

                        // Load DataTable into the destination database
                        if (!await TryBulkInsertDataTableAsync(destinationConnection, sqliteDataTable.TableName, sqliteDataTable))
                        {
                            // Look at the SQLite table and find the missing columns for the destination table
                            _hadBulkInsertError = AreMissingColumnsResolved<T, U>(destinationConnection, sqliteDataTable, tableSchema);

                            // Try to insert again with the newly added columns
                            if (!await TryBulkInsertDataTableAsync(destinationConnection, sqliteDataTable.TableName, sqliteDataTable, false))
                            {
                                _hadBulkInsertError = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static bool AreMissingColumnsResolved<T, U>(T destinationConnection, DataTable sqliteDataTable, string tableSchema)
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            using (SQLiteConnection sqliteDebugConnection = new SQLiteConnection($"Data Source={_masterDbFilepath}"))
            {
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
                        addColumn = $"ALTER TABLE {tableSchema}{sqliteDataTable.TableName} ADD {missingColumn.ColumnName} BIGINT {isNullable}";
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
                        using (U addColumnCommand = new U())
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

            return false;
        }

        private static async Task LoadEnglishTranslationsAsync<T, U>(string connectionString) 
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            using (HttpClient client = new HttpClient())
            {
                string repoName = "FabulousCupcake/umamusume-db-translate";
                string branchName = "master";
                string uri = $"https://api.github.com/repos/{repoName}/git/trees/{branchName}?recursive=1";
                GithubRepoRoot response = await GithubUtility.GetGithubResponseAsync<GithubRepoRoot>(uri, client);

                List<Tree> translatedCsvs = response.Trees.FindAll(file => file.Path.Contains("src/data/") && file.Path.Contains(".csv"));

                Directory.CreateDirectory(@$"{Environment.CurrentDirectory}/src/data"); // prepare storage

                using (T destinationConnection = new T())
                {
                    destinationConnection.ConnectionString = connectionString;
                    destinationConnection.Open();

                    using (U truncateCommand = new U())
                    {
                        // MSSQL uses a separate schema name than the database
                        string tableSchema = "dbo.";
                        if (typeof(T) == typeof(MySqlConnection))
                        {
                            tableSchema = "";
                        }

                        // Start with a clean slate
                        truncateCommand.CommandText = $"TRUNCATE TABLE {tableSchema}text_data_english";
                        truncateCommand.Connection = destinationConnection;
                        truncateCommand.ExecuteNonQuery();

                        foreach (Tree tree in translatedCsvs)
                        {
                            string localFilepath = @$"{Environment.CurrentDirectory}/{tree.Path}";

                            GithubUtility.DownloadRemoteFile(repoName, branchName, tree.Path, localFilepath);

                            /* Load CSV into a DataTable */
                            CsvConfiguration config = new CsvConfiguration(CultureInfo.InvariantCulture)
                            {
                                BadDataFound = null,
                                MissingFieldFound = null
                            };

                            using (StreamReader reader = new StreamReader(localFilepath, Encoding.UTF8))
                            {
                                using (CsvReader csv = new CsvReader(reader, config))
                                {
                                    using (CsvDataReader dr = new CsvDataReader(csv))
                                    {
                                        DataTable dataTable = new DataTable();

                                        try
                                        {
                                            dataTable.Load(dr);
                                        }
                                        catch (CsvHelperException ex)
                                        {
                                            // move onto the next .csv
                                            _hadBulkInsertError = true;
                                            Console.WriteLine(ex.Message);

                                            continue;
                                        }

                                        if (dataTable.Columns.Count != 2)
                                        {
                                            Console.WriteLine($"ERROR: Incorrect column count for \"{tree.Path}\". "
                                                + $"Found {dataTable.Columns.Count} column(s)");

                                            // move onto the next .csv
                                            _hadBulkInsertError = true;
                                            continue;
                                        }

                                        /* Push new info into destination database */
                                        if (!await TryBulkInsertDataTableAsync(destinationConnection, "text_data_english", dataTable, false))
                                        {
                                            _hadBulkInsertError = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }

        private static List<string> SelectTableNames<T>(T connection) where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(SqlConnection))
                {
                    return _sqlServerUtility.SelectTableNames(connection as SqlConnection);
                }
                else if (typeof(T) == typeof(MySqlConnection))
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
                if (typeof(T) == typeof(SqlConnection))
                {
                    return _sqlServerUtility.SelectColumnMetadata(connection as SqlConnection, tableName);
                }
                else if (typeof(T) == typeof(MySqlConnection))
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

        private static async Task<bool> TryBulkInsertDataTableAsync<T>(T connection, string tableName, DataTable dataTable, bool isFirstAttempt = true)
            where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(SqlConnection))
                {
                    return await _sqlServerUtility.TryBulkInsertDataTableAsync(connection as SqlConnection, tableName, dataTable, isFirstAttempt);
                }
                else if (typeof(T) == typeof(MySqlConnection))
                {
                    return await _mySqlUtility.TryBulkInsertDataTableAsync(connection as MySqlConnection, tableName, dataTable, isFirstAttempt);
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
            if (_aspNetCoreEnvironment == "Development" && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
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

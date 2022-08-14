using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

using MySqlConnector;

using UmaMusumeLoadSqlData.Models;
using UmaMusumeLoadSqlData.Utilities;

namespace UmaMusumeLoadSqlData
{
    public class Program
    {
        #region Private Class Variables
        private static readonly List<SqliteMasterRecord> _sqliteTableNames = new List<SqliteMasterRecord>();
        private static readonly List<SqliteMasterRecord> _sqliteIndexNames = new List<SqliteMasterRecord>();
        private static readonly List<DataTable> _sqliteDataTables = new List<DataTable>();
        private static readonly SqliteUtility _sqliteUtility = new SqliteUtility();
        private static readonly SqlServerUtility _sqlServerUtility = new SqlServerUtility();
        private static readonly MySqlUtility _mySqlUtility = new MySqlUtility();
        private static DataTable _dataTable = new DataTable();
        private static bool _hadBulkInsertError = false;
        private static string _masterDbFilepath = @$"{Environment.CurrentDirectory}\master.mdb";
        private const string TRANSLATED_REPO_NAME = "noccu/umamusu-translate";
        private const string TRANSLATED_BRANCH_NAME = "master";
        #endregion

        #region Command Line Arg Class Variables
        public static string AspNetCoreEnvironment { get; private set; }
        private static string _repoName;
        private static string _branchName;
        private static string _mySqlConnectionString;
        private static string _sqlServerConnectionString;
        #endregion

        static void Main(string[] args)
        {
            AspNetCoreEnvironment = args[0];
            _repoName = args[1];
            _branchName = args[2];
            _mySqlConnectionString = args[3];
            _sqlServerConnectionString = args[4];

            MainAsync().GetAwaiter().GetResult();
        }

        #region Actual Main Method
        static async Task MainAsync()
        {
            Console.WriteLine($">>> Now running in \"{AspNetCoreEnvironment}\" <<<\n");

            try
            {
                if (AspNetCoreEnvironment == "Development" && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    _masterDbFilepath 
                        = @$"{Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)}\AppData\LocalLow\Cygames\umamusume\master\master.mdb";
                }
                else
                {
                    // Download master.mdb file from remote GitHub repo
                    await GithubUtility.DownloadRemoteFileAsync(_repoName, _branchName, "master.mdb", _masterDbFilepath);
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

                    // Get raw data index names needed to loop
                    _sqliteUtility.SqliteIndexScript(connection, _sqliteIndexNames);

                    // Loop through each table and pull all available data
                    _sqliteUtility.LoadSqliteDataTables(connection, _sqliteTableNames, _sqliteDataTables);
                }

                /* Import data from SQLite into the provided database(s) below */
                if (!string.IsNullOrEmpty(_mySqlConnectionString) && _mySqlConnectionString != "N/A")
                {
                    await SqlDestinationAsync<MySqlConnection, MySqlCommand>(_mySqlConnectionString).ConfigureAwait(false);
                    await LoadEnglishTranslationsAsync<MySqlConnection, MySqlCommand>(_mySqlConnectionString).ConfigureAwait(false);
                }

                if (!string.IsNullOrEmpty(_sqlServerConnectionString) && _sqlServerConnectionString != "N/A")
                {
                    await SqlDestinationAsync<SqlConnection, SqlCommand>(_sqlServerConnectionString).ConfigureAwait(false);
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
        #endregion

        #region Private Methods
        private static async Task SqlDestinationAsync<T, U>(string connectionString) 
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

                    // Alert the owner of any new tables with their indexes
                    if (_sqliteTableNames.Count > 0)
                    {
                        Console.WriteLine($"WARNING: {_sqliteTableNames.Count} new table(s) found from the master.mdb file...");
                        Console.WriteLine("-----------------------------------------------------------");

                        // NOTE: Local dev strings used and displayed
                        string scaffoldContent = "Scaffold-DbContext 'User Id=root;Password=sa;Host=localhost;Database=umamusume;Character Set=utf8mb4' "
                                + $"Pomelo.EntityFrameworkCore.MySql -OutputDir Models -ContextDir Context -T ";

                        foreach (SqliteMasterRecord table in _sqliteTableNames)
                        {
                            Console.WriteLine();

                            Console.WriteLine($"{table.SqlScript};");
                            foreach (SqliteMasterRecord index in _sqliteIndexNames.Where(i => i.TableName == table.TableName))
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
            try
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
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return false;
        }

        private static async Task LoadEnglishTranslationsAsync<T, U>(string connectionString) 
            where T : IDbConnection, new()
            where U : IDbCommand, new()
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    string uri = $"https://api.github.com/repos/{TRANSLATED_REPO_NAME}/git/trees/{TRANSLATED_BRANCH_NAME}?recursive=1";
                    GithubRepoRoot response = await GithubUtility.GetGithubResponseAsync<GithubRepoRoot>(uri, client);

                    if (response == null)
                    {
                        return; // cannot retrieve info
                    }

                    // Get lists needed for retrieval
                    IEnumerable<string> githubResults = response.Trees
                        .Where(file => file.Path.Contains("translations/"))
                        .Select(p => p.Path);
                    IEnumerable<string> jsonFilePaths = githubResults.Where(file => file.Contains(".json"));
                    IEnumerable<string> subDirectories = githubResults.Where(directory => !directory.Contains(".json"));

                    // clean up
                    response = null;
                    githubResults = null;

                    // Prepare file storage location
                    Directory.CreateDirectory(@$"{Environment.CurrentDirectory}/translations");

                    if (subDirectories.Any())
                    {
                        foreach (string directory in subDirectories)
                        {
                            Directory.CreateDirectory(@$"{Environment.CurrentDirectory}/{directory}");
                        }
                    }

                    // Load translations
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

                            // Start "text_data_english" table with a clean slate
                            truncateCommand.CommandText = $"TRUNCATE TABLE {tableSchema}text_data_english";
                            truncateCommand.Connection = destinationConnection;
                            truncateCommand.ExecuteNonQuery();
                        }

                        _dataTable.Columns.Add("OriginalText");
                        _dataTable.Columns.Add("TranslatedText");
                        int numFiles = 0;

                        Console.WriteLine("\nStarted downloading JSON files...");

                        foreach (string jsonPath in jsonFilePaths)
                        {
                            await DownloadTranslatedJsonFilesAsync(jsonPath);
                            numFiles++;

                            if (numFiles % 20 == 0)
                            {
                                Console.WriteLine($"Downloaded {numFiles} of {jsonFilePaths.Count()} files");
                            }
                        }

                        Console.WriteLine("Finished downloading JSON files");

                        // Push translated data into destination database
                        if (!await TryBulkInsertDataTableAsync(destinationConnection, "text_data_english", _dataTable, false))
                        {
                            _hadBulkInsertError = true;
                        }
                    }
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task DownloadTranslatedJsonFilesAsync(string jsonPath)
        {
            try
            {
                string localFilepath = @$"{Environment.CurrentDirectory}/{jsonPath}";
                await GithubUtility.DownloadRemoteFileAsync(TRANSLATED_REPO_NAME, TRANSLATED_BRANCH_NAME, jsonPath, localFilepath);

                using (StreamReader reader = new StreamReader(localFilepath, Encoding.UTF8))
                {
                    string json = reader.ReadToEnd();
                    JsonObject jsonObject = JsonNode.Parse(json).AsObject();

                    // Load translated JSON based on specified structures
                    if (localFilepath == @$"{Environment.CurrentDirectory}/translations/localify/ui.json")
                    {
                        foreach (KeyValuePair<string, JsonNode> node in jsonObject)
                        {
                            DataRow dr = _dataTable.NewRow();
                            dr["OriginalText"] = node.Key;
                            dr["TranslatedText"] = node.Value.ToString();
                            _dataTable.Rows.Add(dr);
                        }
                    }
                    else if (localFilepath.Contains(@$"{Environment.CurrentDirectory}/translations/mdb/"))
                    {
                        JsonObject textJsonObject = jsonObject.Single(k => k.Key == "text").Value.AsObject();

                        foreach (KeyValuePair<string, JsonNode> node in textJsonObject)
                        {
                            DataRow dr = _dataTable.NewRow();
                            dr["OriginalText"] = node.Key;
                            dr["TranslatedText"] = node.Value.ToString();
                            _dataTable.Rows.Add(dr);
                        }
                    }
                    else
                    {
                        JsonArray textJsonArray = jsonObject.Single(k => k.Key == "text").Value.AsArray();
                        foreach (JsonNode nodeArray in textJsonArray)
                        {
                            DataRow dr = _dataTable.NewRow();
                            IEnumerable<KeyValuePair<string, JsonNode>> textJsonObject = nodeArray.AsObject()
                                .Where(n => n.Key == "jpText" || n.Key == "enText");

                            foreach (KeyValuePair<string, JsonNode> node in textJsonObject)
                            {
                                if (node.Key == "jpText")
                                {
                                    dr["OriginalText"] = node.Value.ToString();
                                }
                                else
                                {
                                    dr["TranslatedText"] = node.Value.ToString();
                                }
                            }

                            _dataTable.Rows.Add(dr);
                        }
                    }
                }

                File.Delete(localFilepath); // clean up
                if (AspNetCoreEnvironment == "Development")
                {
                    Console.WriteLine($"Deleted \"{localFilepath}\"");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
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

        private static async Task<bool> TryBulkInsertDataTableAsync<T>(T connection, string tableName, DataTable sourceDataTable, bool isFirstAttempt = true)
            where T : IDbConnection
        {
            try
            {
                if (typeof(T) == typeof(SqlConnection))
                {
                    return await _sqlServerUtility.TryBulkInsertDataTableAsync(connection as SqlConnection, tableName, sourceDataTable, isFirstAttempt);
                }
                else if (typeof(T) == typeof(MySqlConnection))
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

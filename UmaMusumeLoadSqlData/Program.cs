using System;
using System.Collections.Concurrent;
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
        private static readonly SqliteUtility _sqliteUtility = new SqliteUtility();
        private static readonly SqlServerUtility _sqlServerUtility = new SqlServerUtility();
        private static readonly MySqlUtility _mySqlUtility = new MySqlUtility();
        private static readonly ConcurrentDictionary<string, string> _textDictionary = new ConcurrentDictionary<string, string>();
        private static bool _hadBulkInsertError = false;
        #endregion

        #region Private Const Variables
        private const string TRANSLATED_REPO_NAME = "noccu/umamusu-translate";
        private const string TRANSLATED_BRANCH_NAME = "master";

        private const string SOURCE_MASTER_FILENAME = "master/master.mdb";
        private const string SOURCE_META_FILENAME = "meta";
        #endregion

        #region Command Line Arg Class Variables
        public static string AspNetCoreEnvironment { get; private set; }
        private static string _repoName;
        private static string _branchName;
        private static string _mySqlConnectionString;
        private static string _sqlServerConnectionString;
        public static bool IsVerbose { get; private set; } = false;
        #endregion

        static void Main(string[] args)
        {
            AspNetCoreEnvironment = args[0];
            _repoName = args[1];
            _branchName = args[2];
            _mySqlConnectionString = args[3];

            if (args.Length >=5)
            {
                _sqlServerConnectionString = args[4];
            }

            if (args.Length >=6 && args[5].ToLower() == "verbose")
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

                /* Import data from SQLite into the provided database(s) below */
                if (!string.IsNullOrEmpty(_mySqlConnectionString) && _mySqlConnectionString != "N/A")
                {
                    await LoadEnglishTranslationsAsync<MySqlConnection, MySqlCommand>(_mySqlConnectionString).ConfigureAwait(false);
                }

                if (!string.IsNullOrEmpty(_sqlServerConnectionString) && _sqlServerConnectionString != "N/A")
                {
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
        private static async Task LoadDatabaseData(string dbFilepath, string sourceFilename)
        {
            List<SqliteMasterRecord> sqliteMasterTableNames = new List<SqliteMasterRecord>();
            List<SqliteMasterRecord> sqliteMasterIndexNames = new List<SqliteMasterRecord>();
            List<DataTable> sqliteMasterDataTables = new List<DataTable>();

            /* Pull table info from the SQLite master.mdb */
            using (SQLiteConnection connection = new SQLiteConnection($"Data Source={dbFilepath}"))
            {
                connection.Open();

                // Get raw data table names needed to loop
                _sqliteUtility.SqliteTableNames(connection, sqliteMasterTableNames);

                // Get raw data index names needed to loop
                _sqliteUtility.SqliteIndexScript(connection, sqliteMasterIndexNames);

                // Loop through each table and pull all available data
                _sqliteUtility.LoadSqliteDataTables(connection, sqliteMasterTableNames, sqliteMasterDataTables);
            }

            /* Import data from SQLite into the provided database(s) below */
            if (!string.IsNullOrEmpty(_mySqlConnectionString) && _mySqlConnectionString != "N/A")
            {
                await SqlDestinationAsync<MySqlConnection, MySqlCommand>(sourceFilename, _mySqlConnectionString,
                    sqliteMasterTableNames, sqliteMasterIndexNames, sqliteMasterDataTables).ConfigureAwait(false);
            }

            if (!string.IsNullOrEmpty(_sqlServerConnectionString) && _sqlServerConnectionString != "N/A")
            {
                await SqlDestinationAsync<SqlConnection, SqlCommand>(sourceFilename, _sqlServerConnectionString,
                    sqliteMasterTableNames, sqliteMasterIndexNames, sqliteMasterDataTables).ConfigureAwait(false);
            }
        }

        private static async Task SqlDestinationAsync<T, U>(string sourceFilename, string connectionString, 
            List<SqliteMasterRecord> sqliteMasterTableNames, List<SqliteMasterRecord> sqliteMasterIndexNames, List<DataTable> sqliteMasterDataTables) 
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
                    Console.WriteLine($"\nAttempting to load \"{sourceFilename}\" table data into a MySQL/MariaDB database...\n");
                }
                else
                {
                    Console.WriteLine($"\nAttempting to load \"{sourceFilename}\" table data into a SQL Server database...\n");
                }

                /* Push new info into destination database */
                using (T destinationConnection = new T())
                {
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
                        using (U truncateCommand = new U())
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
                using (SQLiteConnection sqliteDebugConnection = new SQLiteConnection($"Data Source={dbFilepath}"))
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

                        Console.WriteLine("\nStarted downloading JSON translation files...");

                        List<Task> downloadTasks = new List<Task>();
                        int numFiles = 0;

                        // Download translated JSON files in batches
                        foreach (string jsonPath in jsonFilePaths)
                        {
                            // Queue up the download
                            downloadTasks.Add(DownloadTranslatedJsonFilesAsync(jsonPath));
                            numFiles++;

                            // Limit to 200 concurrent downloads and wait until they're all done
                            if (downloadTasks.Count >= 200)
                            {
                                Task.WaitAll(downloadTasks.ToArray());
                                downloadTasks.Clear(); // prep for next batch
                                Console.WriteLine($"Downloaded {numFiles} of {jsonFilePaths.Count()} files");
                            }
                        }

                        Task.WaitAll(downloadTasks.ToArray());

                        Console.WriteLine("Finished downloading JSON translation files");

                        // Load text into database
                        using (DataTable importDataTable = new DataTable())
                        {
                            importDataTable.Columns.Add("OriginalText");
                            importDataTable.Columns.Add("TranslatedText");

                            foreach (KeyValuePair<string, string> textKeyValuePair in _textDictionary)
                            {
                                DataRow dr = importDataTable.NewRow();
                                dr["OriginalText"] = textKeyValuePair.Key;
                                dr["TranslatedText"] = textKeyValuePair.Value;
                                importDataTable.Rows.Add(dr);
                            }

                            // Push translated data into destination database
                            if (!await TryBulkInsertDataTableAsync(destinationConnection, "text_data_english", importDataTable, false))
                            {
                                _hadBulkInsertError = true;
                            }
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

                try
                {
                    using (StreamReader reader = new StreamReader(localFilepath, Encoding.UTF8))
                    {
                        string json = reader.ReadToEnd();
                        JsonObject jsonObject = JsonNode.Parse(json).AsObject();

                        // Load translated JSON based on specified structures
                        if (localFilepath == @$"{Environment.CurrentDirectory}/translations/localify/ui.json")
                        {
                            foreach (KeyValuePair<string, JsonNode> node in jsonObject)
                            {
                                _textDictionary.TryAdd(node.Key, node.Value.ToString());
                            }
                        }
                        else if (localFilepath.Contains(@$"{Environment.CurrentDirectory}/translations/mdb/"))
                        {
                            JsonObject textJsonObject = jsonObject.Single(k => k.Key == "text").Value.AsObject();

                            foreach (KeyValuePair<string, JsonNode> node in textJsonObject)
                            {
                                _textDictionary.TryAdd(node.Key, node.Value.ToString());
                            }
                        }
                        else
                        {
                            JsonArray textJsonArray = jsonObject.Single(k => k.Key == "text").Value.AsArray();
                            foreach (JsonNode nodeArray in textJsonArray)
                            {
                                IEnumerable<KeyValuePair<string, JsonNode>> textJsonObject = nodeArray.AsObject()
                                    .Where(n => n.Key == "jpText" || n.Key == "enText");

                                string originalText = "";
                                string translatedText = "";
                                foreach (KeyValuePair<string, JsonNode> node in textJsonObject)
                                {
                                    if (node.Key == "jpText")
                                    {
                                        originalText = node.Value.ToString();
                                    }
                                    else
                                    {
                                        translatedText = node.Value.ToString();
                                    }
                                }

                                if (string.IsNullOrEmpty(originalText))
                                {
                                    continue; // skip missing key
                                }

                                _textDictionary.TryAdd(originalText, translatedText);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    File.Delete(localFilepath); // clean up
                    if (IsVerbose)
                    {
                        Console.WriteLine($"Deleted \"{localFilepath}\"");
                    }
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

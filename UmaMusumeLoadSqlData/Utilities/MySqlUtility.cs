﻿using System;
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
            List<ColumnMetadata> result = [];

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

            using (MySqlCommand command = new(commandText, connection))
            {
                using MySqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(new ColumnMetadata
                    {
                        ColumnName = reader.GetString(0),
                        ColumnDataType = reader.GetString(1),
                        IsNullable = reader.GetString(2) == "YES"
                    });
                }
            }

            return result;
        }
        #endregion

        #region ISqlDestination Methods
        public List<string> SelectTableNames(MySqlConnection connection)
        {
            List<string> result = [];

            // TODO: Store database name (table_schema) somewhere else
            using (MySqlCommand command = new($"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{connection.Database}'", connection))
            {
                using MySqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    result.Add(reader.GetString(0));
                }
            }

            return result;
        }

        public async Task<bool> TryBulkInsertDataTableAsync(MySqlConnection connection, string tableName, DataTable sourceDataTable, bool isFirstAttempt = true)
        {
            try
            {
                MySqlBulkCopy bulkCopy = new(connection)
                {
                    DestinationTableName = tableName
                };

                // Ensure column order
                foreach (DataColumn column in sourceDataTable.Columns)
                {
                    // Take care of every game table column order
                    if (tableName != "text_data_english")
                    {
                        bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping
                        {
                            DestinationColumn = column.ColumnName,
                            SourceOrdinal = sourceDataTable.Columns[column.ColumnName].Ordinal
                        });
                    }
                }

                await bulkCopy.WriteToServerAsync(sourceDataTable);
                Console.WriteLine($"Successfully loaded rows into \"{tableName}\"");

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
                    Console.WriteLine($"\nERROR: Could not bulk insert for the table \"{tableName}\"\n");
                    Console.WriteLine(ex.Message);

                    // Reference: https://sqlbulkcopy-tutorial.net/columnmapping-does-not-match
                    if (!ex.Message.Contains("The given ColumnMapping does not match up with any column in the source or destination"))
                    {
                        Console.WriteLine(ex.StackTrace + "\n");

                        Console.WriteLine("\nPOSSIBLE SOLUTION: Compare and match the column ordering between the \"DataTable\" variable and destination table.");
                        if (sourceDataTable != null)
                        {
                            Console.WriteLine($"\nDataTable info for \"{tableName}\"");
                            foreach (DataColumn column in sourceDataTable.Columns)
                            {
                                Console.WriteLine($"Column Name: \"{column.ColumnName}\" | Data Type: {column.DataType.Name} | Is Nullable: {column.AllowDBNull}");
                            }
                            Console.WriteLine();
                        }
                    }
                }
            }

            return false;
        }
        #endregion
    }
}

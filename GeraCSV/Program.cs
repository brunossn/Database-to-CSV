using FirebirdSql.Data.FirebirdClient;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Text;
using Npgsql;
using IBM.Data.DB2;

namespace DatabaseToCSV
{
    class Program
    {
        private enum eDatabase
        {
            SQLServer = 1,
            Sqlite = 2,
            Firebird = 3,
            Oracle = 4,
            MySql = 5,
            Access = 6,
            DB2 = 7,
            PostgreSql = 8
        }

        static void Main(string[] args)
        {
            int rowNumber = 0;
            int columnNumber = 0;

            try
            {
                string validationMessage = Validate(args);
                
                if (validationMessage == "")
                {
                    string queryFile = args[0];
                    string csvFile = args[1];
                    string connectionString = args[2];
                    eDatabase database = (eDatabase)Convert.ToInt32(args[3]);
                   
                    using (var connection = GetConnection(database, connectionString))
                    {
                        connection.Open();

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = File.ReadAllText(queryFile, Encoding.GetEncoding("ISO-8859-1"));
                            command.Connection = connection;
                            command.CommandTimeout = 90000;

                            using (var dr = command.ExecuteReader())
                            {
                                using (var sw = new StreamWriter(csvFile, false, Encoding.UTF8))
                                {
                                    while (dr.Read())
                                    {
                                        rowNumber++;

                                        for (columnNumber = 0; columnNumber < dr.FieldCount; columnNumber++)
                                        {
                                            if (columnNumber != 0) sw.Write(";");
                                            sw.Write(dr[columnNumber].ToString().Replace("\n", " ").Replace("\r", " ").Trim());
                                        }

                                        sw.Write("\n");
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine(validationMessage);
                    Console.Read();
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Error on row {0}, column {1}.", rowNumber.ToString(), columnNumber.ToString());
                Console.WriteLine("\n\nError message: " + ex.Message);
                Console.WriteLine("\n\nDetails: " + ex.ToString());
                Console.Read();
            }
        }

        private static DbConnection GetConnection(eDatabase tipoBanco, string stringConexao)
        {
            switch (tipoBanco)
            {
                case eDatabase.SQLServer:
                    return new SqlConnection(stringConexao);
                case eDatabase.Sqlite:
                    return new SQLiteConnection(stringConexao);
                case eDatabase.Firebird:
                    return new FbConnection(stringConexao);
                case eDatabase.Oracle:
                    return new OracleConnection(stringConexao);
                case eDatabase.MySql:
                    return new MySqlConnection(stringConexao);
                case eDatabase.Access:
                    return new OleDbConnection(stringConexao);
                case eDatabase.DB2:
                    return new DB2Connection(stringConexao);
                case eDatabase.PostgreSql:
                    return new NpgsqlConnection(stringConexao);
                default:
                    throw new Exception("Unspecified database type.");
            }
        }

        public static string Validate(string[] args)
        {
            if(args.Length != 4)
                return "Run this tool again with four parameters:\n[1] - Full path of a .SQL file with a query\n" +
                    "[2] - Full path of the .CSV file that will be generated\n" + 
                    "[3] - Connectionstring (get support in connectionstrings.com)\n" +
                    "[4] - Databse type (1 - SQL Server / 2 - SQLite / 3 - Firebird / 4 - Oracle / 5 - MySql / 6 - Access / 7 - IBM DB2 / 8 - PostgreSQL)";
            else
            {
                string file = args[0];

                if(!File.Exists(file))
                    return string.Format("File not found in {0}!", file);
            }

            return "";
        }
    }
}

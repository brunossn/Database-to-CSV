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
        private const int TIMEOUT_MILESSECONDS = 90000;

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
                if (!Validate(args)) return;
                
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
                        command.CommandTimeout = TIMEOUT_MILESSECONDS;

                        using (var dr = command.ExecuteReader())
                        using (var sw = new StreamWriter(csvFile, false, Encoding.UTF8))
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
            catch(Exception ex)
            {
                Console.WriteLine("Error on row {0}, column {1}.", rowNumber.ToString(), columnNumber.ToString());
                Console.WriteLine("\n\nError message: " + ex.Message);
                Console.WriteLine("\n\nDetails: " + ex.ToString());
                Console.Read();
            }
        }

        private static DbConnection GetConnection(eDatabase databaseType, string connectionString)
        {
            switch (databaseType)
            {
                case eDatabase.SQLServer:
                    return new SqlConnection(connectionString);
                case eDatabase.Sqlite:
                    return new SQLiteConnection(connectionString);
                case eDatabase.Firebird:
                    return new FbConnection(connectionString);
                case eDatabase.Oracle:
                    return new OracleConnection(connectionString);
                case eDatabase.MySql:
                    return new MySqlConnection(connectionString);
                case eDatabase.Access:
                    return new OleDbConnection(connectionString);
                case eDatabase.DB2:
                    return new DB2Connection(connectionString);
                case eDatabase.PostgreSql:
                    return new NpgsqlConnection(connectionString);
                default:
                    throw new Exception("Unspecified database type.");
            }
        }

        public static bool Validate(string[] args)
        {
            var message = "";

            if(args.Length != 4)
                message = "Run this tool again with four parameters:\n[1] - Full path of a .SQL file with a query\n" +
                    "[2] - Full path of the .CSV file that will be generated\n" + 
                    "[3] - Connectionstring (get support in connectionstrings.com)\n" +
                    "[4] - Database type (1 - SQL Server / 2 - SQLite / 3 - Firebird / 4 - Oracle / 5 - MySql / 6 - Access / 7 - IBM DB2 / 8 - PostgreSQL)";
            else
            {
                string file = args[0];

                if(!File.Exists(file))
                    message = string.Format("File not found in {0}!", file);
            }

            if(message != "")
            {
                Console.WriteLine(message);
                Console.Read();
                return false;
            }

            return true;
        }
    }
}

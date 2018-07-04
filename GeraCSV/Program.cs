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
//using IBM.Data.DB2;

namespace GeraCSV
{
    class Program
    {
        private enum eTipoBanco
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
            int linhaAtual = 0;
            int colunaAtual = 0;

            try
            {
                string mensagemValida = valida(args);
                
                if (mensagemValida == "")
                {
                    string arquivoQuery = args[0];
                    string saidaCSV = args[1];
                    string stringConexao = args[2];
                    eTipoBanco tipoBanco = (eTipoBanco)Convert.ToInt32(args[3]);
                   
                    using (var conexao = GetConnection(tipoBanco, stringConexao))
                    {
                        conexao.Open();

                        using (var comando = conexao.CreateCommand())
                        {
                            comando.CommandText = File.ReadAllText(arquivoQuery, Encoding.GetEncoding("ISO-8859-1"));
                            comando.Connection = conexao;
                            comando.CommandTimeout = 90000;

                            using (var dr = comando.ExecuteReader())
                            {
                                using (var sw = new StreamWriter(saidaCSV, false, Encoding.UTF8))
                                {
                                    while (dr.Read())
                                    {
                                        linhaAtual++;

                                        for (colunaAtual = 0; colunaAtual < dr.FieldCount; colunaAtual++)
                                        {
                                            if (colunaAtual != 0) sw.Write(";");
                                            sw.Write(dr[colunaAtual].ToString().Replace("\n", " ").Replace("\r", " ").Trim());
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
                    Console.WriteLine(mensagemValida);
                    Console.Read();
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine("Erro na linha {0}, coluna {1}.", linhaAtual.ToString(), colunaAtual.ToString());
                Console.WriteLine("\n\nDescrição: " + ex.Message);
                Console.WriteLine("\n\nGeral: " + ex.ToString());
                Console.Read();
            }
        }

        private static DbConnection GetConnection(eTipoBanco tipoBanco, string stringConexao)
        {
            switch (tipoBanco)
            {
                case eTipoBanco.SQLServer:
                    return new SqlConnection(stringConexao);
                case eTipoBanco.Sqlite:
                    return new SQLiteConnection(stringConexao);
                case eTipoBanco.Firebird:
                    return new FbConnection(stringConexao);
                case eTipoBanco.Oracle:
                    return new OracleConnection(stringConexao);
                case eTipoBanco.MySql:
                    return new MySqlConnection(stringConexao);
                case eTipoBanco.Access:
                    return new OleDbConnection(stringConexao);
                //case eTipoBanco.DB2:
                //    return new DB2Connection(stringConexao);
                case eTipoBanco.PostgreSql:
                    return new NpgsqlConnection(stringConexao);
                default:
                    throw new Exception("Tipo de banco de dados não especificado.");
            }
        }

        public static string valida(string[] args)
        {
            if(args.Length != 4)
                return "Execute o sistema passando quatro parâmetros:\n[1] - Arquivo TXT contendo a query\n" +
                    "[2] - Caminho do arquivo CSV que será gerado\n[3] - String de conexão com o banco de dados\n" +
                    "[4] - Tipo do banco de dados (1 - SQL Server / 2 - SQLite / 3 - Firebird / 4 - Oracle / 5 - MySql / 6 - Access / 7 - IBM DB2 / 8 - PostgreSQL)";
            else
            {
                string caminho = args[0];

                if(!File.Exists(caminho))
                    return string.Format("Arquivo {0} não localizado!", caminho);
            }

            return "";
        }
    }
}

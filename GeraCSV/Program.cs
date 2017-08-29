﻿using FirebirdSql.Data.FirebirdClient;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Text;

namespace GeraCSV
{
    class Program
    {
        private enum eTipoBanco
        {
            SQLServer = 1,
            Sqlite = 2,
            Firebird = 3
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
                    
                    var query = File.ReadAllText(arquivoQuery);
                    var saida = new StringBuilder();

                    // Conecta ao SQL
                    DbConnection conexao;
                    DbCommand comando;
                    switch(tipoBanco)
                    {
                        case eTipoBanco.SQLServer:
                            conexao = new SqlConnection(stringConexao);
                            comando = new SqlCommand();
                            break;
                        case eTipoBanco.Sqlite:
                            conexao = new SQLiteConnection(stringConexao);
                            comando = new SQLiteCommand();
                            break;
                        case eTipoBanco.Firebird:
                            conexao = new FbConnection(stringConexao);
                            comando = new FbCommand();
                            break;
                        default:
                            throw new Exception("Tipo de banco de dados não especificado.");
                    }
                    
                    conexao.Open();
                    comando.CommandText = query;
                    comando.Connection = conexao;
                    comando.CommandTimeout = 90000;

                    using (DbDataReader dr = comando.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            linhaAtual++;

                            for (colunaAtual = 0; colunaAtual < dr.FieldCount; colunaAtual++)
                            {
                                if (colunaAtual != 0) saida.Append(";");
                                saida.Append(dr[colunaAtual].ToString().Trim());
                            }

                            saida.Append("\n");
                        }
                    }
                    
                    // Saída
                    File.WriteAllText(saidaCSV, saida.ToString().Trim(), Encoding.UTF8);

                    conexao.Close();
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

        public static string valida(string[] args)
        {
            string retorno = "";

            if(args.Length != 4)
            {
                retorno = "Execute o sistema passando quatro parâmetros:\n[1] - Arquivo TXT contendo a query\n" +
                    "[2] - Caminho do arquivo CSV que será gerado\n[3] - String de conexão com o banco de dados\n" +
                    "[4] - Tipo do banco de dados (1 - SQL Server / 2 - SQLite / 3 - Firebird)";
            }
            else
            {
                string caminho = args[0];

                if(!File.Exists(caminho))
                {
                    retorno = string.Format("Arquivo {0} não localizado!", caminho);
                }
            }

            return retorno;
        }
    }
}
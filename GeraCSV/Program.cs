using System;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace GeraCSV
{
    class Program
    {
        static void Main(string[] args)
        {
            int linhaAtual = 0;
            int colunaAtual = 0;

            try
            {
                string mensagemValida = valida(args);

                if (mensagemValida == "")
                {
                    string arquivoEntrada = args[0];
                    string arquivoSaida = args[1];
                    string stringConexao = args[2];
                    string query = File.ReadAllText(arquivoEntrada);
                    StringBuilder saida = new StringBuilder();

                    // Conecta ao SQL
                    SqlConnection sqlConexao = new SqlConnection(stringConexao);
                    sqlConexao.Open();

                    SqlCommand sqlComando = new SqlCommand(query, sqlConexao);
                    sqlComando.CommandTimeout = 90000;

                    SqlDataReader sqlDataReader = sqlComando.ExecuteReader();
                    

                    // Cria CSV
                    while (sqlDataReader.Read())
                    {
                        linhaAtual++;

                        for (colunaAtual = 0; colunaAtual < sqlDataReader.FieldCount; colunaAtual++)
                        {
                            if (colunaAtual != 0) saida.Append(";");
                            saida.Append(sqlDataReader[colunaAtual].ToString().Trim());
                        }

                        saida.Append("\n");
                    }

                    // Saída
                    File.WriteAllText(arquivoSaida, saida.ToString().Trim(), Encoding.UTF8);

                    sqlConexao.Close();
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

            if(args.Length != 3)
            {
                retorno = "Execute o sistema passando três parâmetros:\n[1] - Arquivo contendo a query\n[2] - Arquivo CSV que será gerado;\n[3] - String de conexão com o banco de dados";
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

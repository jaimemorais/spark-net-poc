using Microsoft.Spark;
using Microsoft.Spark.Sql;
using System;
using System.Diagnostics;
using static Microsoft.Spark.Sql.Functions;

namespace SparkNetPOC
{


    // Config :
    // - Instalar Spark https://dotnet.microsoft.com/learn/data/spark-tutorial/install-spark
    //   Atencao para a versao do Spark compativel com o Spark.NET https://github.com/dotnet/spark#supported-apache-spark
    // - Instalar .NET para Spark https://dotnet.microsoft.com/learn/data/spark-tutorial/install-worker

    // Para rodar :
    // spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local bin\Debug\netcoreapp3.1\microsoft-spark-2.4.x-0.12.1.jar dotnet bin\Debug\netcoreapp3.1\SparkNetPOC.dll


    public static class Program
    {

        public static void Main(string[] args)
        {

            // arquivo usado : https://www.kaggle.com/gbonesso/b3-stock-quotes/data?select=COTAHIST_A2009_to_A2020_P.csv
            // essa poc calcula o preco medio da acao nesse periodo 

            SparkConf sparkConf = new SparkConf();
            sparkConf.SetMaster("local[*]");  // '*' indica pra usar todos os cores

            SparkSession spark = SparkSession
                .Builder()
                .Config(sparkConf)
                .AppName("SparkNetPOC")
                .GetOrCreate();


            Stopwatch sw = new Stopwatch();
            sw.Start();


            DataFrame dataFrameGeral = spark.Read()
               .Schema("vazio STRING, TIPREG STRING,DATPRE STRING,CODBDI STRING,CODNEG STRING,TPMERC STRING,NOMRES STRING,ESPECI STRING," +
                        "PRAZOT STRING,MODREF STRING,PREABE STRING,PREMAX STRING,PREMIN STRING,PREMED STRING,PREULT STRING,PREOFC STRING," +
                        "PREOFV STRING,TOTNEG STRING,QUATOT STRING," +
                        "VOLTOT STRING,PREEXE STRING,INDOPC STRING,DATVEN STRING,FATCOT STRING,PTOEXE STRING,CODISI STRING,DISMES STRING")
               .Csv(@"C:\InternetDownloads\10318_1101179_compressed_COTAHIST_A2009_to_A2020_P.csv\COTAHIST_A2009_to_A2020_P.csv");


            DataFrame dataFrameColunasUteis = dataFrameGeral
                .Drop("vazio", "TIPREG", "DATPRE", "CODBDI", "TPMERC", "NOMRES", "ESPECI", "PRAZOT", "MODREF", "PREABE", "PREMIN",
                      "PREMED", "PREULT", "PREOFC", "PREOFV", "TOTNEG", "QUATOT", "VOLTOT", "PREEXE", "INDOPC", "DATVEN", "FATCOT", "PTOEXE", "CODISI", "DISMES");

            DataFrame dataFrameFiltro = dataFrameColunasUteis
                .Filter("CODNEG = 'ITSA3' OR CODNEG = 'ABEV3' OR CODNEG = 'PETR4'");

            DataFrame dataFrameFinal = dataFrameFiltro
                .GroupBy("CODNEG")
                .Agg(Avg("PREMAX"));

            dataFrameFinal.Show();


            spark.Stop();

            sw.Stop();
            Console.WriteLine("Tempo = " + sw.ElapsedMilliseconds);
        }


    }
}

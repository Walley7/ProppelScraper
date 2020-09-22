using CSACore.Core;
using CSACore.Utility;
using HtmlAgilityPack;
using MySql.Data.MySqlClient;
using ProppelScraper.Scraping;
using ScrapySharp.Network;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;



namespace ProppelScraper {

    class Program {
        //================================================================================
        public const string                     CONFIGURATION_PATH = "ProppelScraper.json";
        public const string                     LOG_PATH = "Log.log";

        //--------------------------------------------------------------------------------
        public enum EDatabaseMode {
            MYSQL,
            SQLITE
        }


        //================================================================================
        private static EDatabaseMode            sDatabaseMode;
        private static bool                     sRecordLookup;


        //================================================================================
        //--------------------------------------------------------------------------------
        static void Main(string[] args) {
            // Initialise - CSA
            try {
                CSA.Initialise(CONFIGURATION_PATH);
                CSA.OpenLog(LOG_PATH);
            }
            catch (Exception e) {
                Console.WriteLine($"Failed to initialise: {e.Message}");
                return;
            }

            // Configuration
            string connectionString = CSA.Setting("ConnectionString");
            string mode = CSA.Setting("Mode");
            sDatabaseMode = (CSA.Setting("DatabaseMode") == "mysql" ? EDatabaseMode.MYSQL : EDatabaseMode.SQLITE);
            sRecordLookup = UConvert.FromString<bool>(CSA.Setting("RecordLookup"));
            string proxyIP = CSA.Setting("ProxyIP", false);
            string proxyUsername = CSA.Setting("ProxyUsername", false);
            string proxyPassword = CSA.Setting("ProxyPassword", false);
            string[][] propertyRanges = CSA.Array2DSetting("PropertyRanges");
            string[][] reportRanges = CSA.Array2DSetting("ReportRanges");
            int generatedThreads = UConvert.FromString<int>(CSA.Setting("GeneratedThreads", false), 0);
            string[] generatedPropertyRange = CSA.ArraySetting("GeneratedPropertyRange");
            string[] generatedReportRange = CSA.ArraySetting("GeneratedReportRange");

            // Generated ranges
            if (generatedThreads > 0 && generatedPropertyRange.Length > 0)
                propertyRanges = GenerateRanges(generatedThreads, generatedPropertyRange);
            if (generatedThreads > 0 && generatedReportRange.Length > 0)
                reportRanges = GenerateRanges(generatedThreads, generatedReportRange);

            // Initialise database
            InitialiseDatabase(connectionString);

            // Tests
            PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, proxyUsername, proxyPassword, "vic");
            AddressData address = scraper.ScrapeAddress(1738757); //1754802
            Console.WriteLine(address);
            return;

            /*SQLiteConnection connection = new SQLiteConnection(connectionString);
            connection.Open();
            address.Save(connection);
            connection.Dispose();*/

            /*ReportScraper scraper = new ReportScraper(connectionString, "vic");
            AddressData address = scraper.ScrapeAddress(4619); // 10000, 2910325, 3000000
            Console.WriteLine(address);*/

            // Scrape
            if (mode.Contains("reports")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("REPORTS:");
                ScrapeAddresses(StartReportScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, reportRanges);
            }
            if (mode.Contains("properties")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("PROPERTIES:");
                ScrapeAddresses(StartPropertyScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, propertyRanges);
            }

            // Shutdown
            CSA.Shutdown();
        }


        // DATABASE ================================================================================
        //--------------------------------------------------------------------------------
        static void InitialiseDatabase(string connectionString) {
            // Connect
            DbConnection connection;
            if (DatabaseMode == EDatabaseMode.MYSQL)
                connection = new MySqlConnection(connectionString);
            else
                connection = new SQLiteConnection(connectionString);
            connection.Open();

            // Tables
            AddressData.InitialiseDatabase(connection);

            // Close
            connection.Dispose();
        }

        //--------------------------------------------------------------------------------
        public static EDatabaseMode DatabaseMode { get => sDatabaseMode; }
        public static bool DatabaseIsMySQL { get => sDatabaseMode == EDatabaseMode.MYSQL; }
        public static bool DatabaseIsSQLite { get => sDatabaseMode == EDatabaseMode.SQLITE; }
        public static bool RecordLookup { get => sRecordLookup; }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        static void ScrapeAddresses(Func<string, string, string, string, string, int, int, Task<string>> scrapeTaskFunction, string connectionString,
                                    string proxyIP, string proxyUsername, string proxyPassword, string[][] ranges)
        {
            // Scrape
            List<Task<string>> tasks = new List<Task<string>>();
            foreach (string[] r in ranges) {
                tasks.Add(scrapeTaskFunction(connectionString, proxyIP, proxyUsername, proxyPassword, r[0], UConvert.FromString<int>(r[1]), UConvert.FromString<int>(r[2])));
            }

            // Wait
            Task.WaitAll(tasks.ToArray());

            // Results
            foreach (Task<string> t in tasks) {
                CSA.Logger.LogInfo($"TASK COMPLETE: {t.Result}");
            }
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartReportScrapeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                ReportScraper scraper = new ReportScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, minimumID, maximumID);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartPropertyScrapeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, minimumID, maximumID);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }


        // RANGES ================================================================================
        //--------------------------------------------------------------------------------
        private static string[][] GenerateRanges(int threads, string[] range) {
            // Range
            string state = range[0];
            int lowerRange = UConvert.FromString<int>(range[1]);
            int upperRange = UConvert.FromString<int>(range[2]);
            int threadRange = (int)Math.Ceiling((decimal)(upperRange + 1 - lowerRange) / (decimal)threads);

            // Generate
            List<string[]> ranges = new List<string[]>();
            for (int i = lowerRange; i <= upperRange; i += threadRange) {
                ranges.Add(new string[] { state, i.ToString(), (i + threadRange - 1).ToString() });
                //CSA.Logger.LogInfo($"Range: {state}, {i}, {Math.Min(i + threadRange - 1, upperRange)}");
            }

            // Return
            return ranges.ToArray();
        }
    }

}

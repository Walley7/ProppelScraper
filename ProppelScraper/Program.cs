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
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;



namespace ProppelScraper {

    class Program {
        //================================================================================
        public const string                     CONFIGURATION_PATH = "ProppelScraper.json";
        public const string                     LOGGING_PATH = "Logs";
        public const string                     LOG_FILENAME = "Log.log";

        //--------------------------------------------------------------------------------
        public enum EDatabaseMode {
            MYSQL,
            SQLITE
        }


        //================================================================================
        private static bool                     sVerboseLogging;
        private static EDatabaseMode            sDatabaseMode;
        private static bool                     sRecordLookup;


        //================================================================================
        //--------------------------------------------------------------------------------
        static void Main(string[] args) {
            // Initialise - CSA
            try {
                CSA.Initialise(CONFIGURATION_PATH);
                CSA.OpenLog(Path.Combine(LOGGING_PATH, LOG_FILENAME));
            }
            catch (Exception e) {
                Console.WriteLine($"Failed to initialise: {e.Message}");
                return;
            }

            // Configuration
            string connectionString = CSA.Setting("ConnectionString");
            sVerboseLogging = UConvert.FromString<bool>(CSA.Setting("VerboseLogging"), true);
            string mode = CSA.Setting("Mode");
            sDatabaseMode = (CSA.Setting("DatabaseMode") == "mysql" ? EDatabaseMode.MYSQL : EDatabaseMode.SQLITE);
            sRecordLookup = UConvert.FromString<bool>(CSA.Setting("RecordLookup"));
            string proxyIP = CSA.Setting("ProxyIP", false);
            string proxyUsername = CSA.Setting("ProxyUsername", false);
            string proxyPassword = CSA.Setting("ProxyPassword", false);
            int threads = UConvert.FromString<int>(CSA.Setting("Threads", false), 0);
            string propertyState = CSA.Setting("PropertyState", false);
            int propertyMinimumID = UConvert.FromString<int>(CSA.Setting("PropertyMinimumID", false), 0);
            int propertyMaximumID = UConvert.FromString<int>(CSA.Setting("PropertyMaximumID", false), 0);
            string reportState = CSA.Setting("ReportState", false);
            int reportMinimumID = UConvert.FromString<int>(CSA.Setting("ReportMinimumID", false), 0);
            int reportMaximumID = UConvert.FromString<int>(CSA.Setting("ReportMaximumID", false), 0);

            // Initialise database
            InitialiseDatabase(connectionString);

            // Tests
            /*PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, proxyUsername, proxyPassword, "qld");
            AddressData address = scraper.ScrapeAddress(33276);
            Console.WriteLine(address);
            return;*/

            /*ReportScraper scraper = new ReportScraper(connectionString, proxyIP, proxyUsername, proxyPassword, "vic");
            AddressData address = scraper.ScrapeAddress(572638); // 25541
            Console.WriteLine(address);
            return;*/

            /*SQLiteConnection connection = new SQLiteConnection(connectionString);
            connection.Open();
            address.Save(connection);
            connection.Dispose();*/

            // Scrape
            if (mode.Contains("properties")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("PROPERTIES:");
                //ScrapeAddresses(StartPropertyScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, propertyRanges);
                ScrapeAddresses(StartPropertyScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, threads, propertyState, propertyMinimumID, propertyMaximumID);
            }
            if (mode.Contains("reports")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("REPORTS:");
                //ScrapeAddresses(StartReportScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, reportRanges);
                ScrapeAddresses(StartReportScrapeTask, connectionString, proxyIP, proxyUsername, proxyPassword, threads, reportState, reportMinimumID, reportMaximumID);
            }

            // Shutdown
            CSA.Shutdown();
        }


        // DATABASE ================================================================================
        //--------------------------------------------------------------------------------
        public static bool VerboseLogging { get => sVerboseLogging; }


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
        static void ScrapeAddresses(Func<string, string, string, string, string, IDProvider, Task<string>> scrapeTaskFunction, string connectionString,
                                    string proxyIP, string proxyUsername, string proxyPassword, int threads, string state, int minimumID, int maximumID)
        {
            // ID provider
            IDProvider idProvider = new IDProvider(minimumID, maximumID);

            // Scrape
            List<Task<string>> tasks = new List<Task<string>>();
            for (int i = 0; i < threads; ++i) {
                tasks.Add(scrapeTaskFunction(connectionString, proxyIP, proxyUsername, proxyPassword, state, idProvider));
            }

            // Wait
            Task.WaitAll(tasks.ToArray());

            // Results
            foreach (Task<string> t in tasks) {
                CSA.Logger.LogInfo($"TASK COMPLETE: {t.Result}");
            }
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartPropertyScrapeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, IDProvider idProvider) {
            return Task.Factory.StartNew(() => {
                PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, idProvider);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartReportScrapeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, IDProvider idProvider) {
            return Task.Factory.StartNew(() => {
                ReportScraper scraper = new ReportScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, idProvider);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }


        // SCRAPING - RANGE BASED ================================================================================
        //--------------------------------------------------------------------------------
        static void ScrapeAddressesByRange(Func<string, string, string, string, string, int, int, Task<string>> scrapeTaskFunction, string connectionString,
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
        static Task<string> StartPropertyScrapeByRangeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, minimumID, maximumID);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartReportScrapeByRangeTask(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                ReportScraper scraper = new ReportScraper(connectionString, proxyIP, proxyUsername, proxyPassword, state, minimumID, maximumID);
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
                ranges.Add(new string[] { state, i.ToString(), (Math.Min(i + threadRange - 1, upperRange)).ToString() });
                //CSA.Logger.LogInfo($"Range: {state}, {i}, {Math.Min(i + threadRange - 1, upperRange)}");
            }

            // Return
            return ranges.ToArray();
        }
    }

}

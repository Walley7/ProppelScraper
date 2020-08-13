using CSACore.Core;
using CSACore.Utility;
using ProppelScraper.Scraping;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;



namespace ProppelScraper {

    class Program {
        //================================================================================
        public const string                     CONFIGURATION_PATH = "ProppelScraper.json";
        public const string                     LOG_PATH = "Log.log";


        //================================================================================
        //--------------------------------------------------------------------------------
        static void Main(string[] args) {
            //sql mini portable
            //distinguish page not found from property not found
            //add image url scraping (use single scrape for testing - do this for page not found distinguishing too)


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
            string connectionString = CSA.Setting("SQLiteConnectionString");
            string mode = CSA.Setting("Mode");
            string proxyIP = CSA.Setting("ProxyIP", false);
            string[][] propertyRanges = CSA.Array2DSetting("PropertyRanges");
            string[][] reportRanges = CSA.Array2DSetting("ReportRanges");

            // Initialise database
            InitialiseDatabase(connectionString);


            /*PropertyScraper scraper = new PropertyScraper(connectionString, "vic");
            AddressData address = scraper.ScrapeAddress(764999);
            Console.WriteLine(address);*/

            /*SQLiteConnection connection = new SQLiteConnection(connectionString);
            connection.Open();
            address.Save(connection);
            connection.Dispose();*/


            /*ReportScraper scraper = new ReportScraper(connectionString, "vic");
            AddressData address = scraper.ScrapeAddress(2910325); // 10000, 2910325, 3000000
            Console.WriteLine(address);*/


            // Scrape
            if (mode.Contains("reports")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("REPORTS:");
                ScrapeAddresses(StartReportScrapeTask, connectionString, proxyIP, reportRanges);
            }
            if (mode.Contains("properties")) {
                CSA.Logger.LogBreak();
                CSA.Logger.LogInfo("PROPERTIES:");
                ScrapeAddresses(StartPropertyScrapeTask, connectionString, proxyIP, propertyRanges);
            }

            // Shutdown
            CSA.Shutdown();
        }


        // DATABASE ================================================================================
        //--------------------------------------------------------------------------------
        static void InitialiseDatabase(string connectionString) {
            // Connect
            SQLiteConnection connection = new SQLiteConnection(connectionString);
            connection.Open();

            // Tables
            AddressData.InitialiseDatabase(connection);

            // Close
            connection.Dispose();
        }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        static void ScrapeAddresses(Func<string, string, string, int, int, Task<string>> scrapeTaskFunction, string connectionString, string proxyIP, string[][] ranges) {
            // Scrape
            List<Task<string>> tasks = new List<Task<string>>();
            foreach (string[] r in ranges) {
                tasks.Add(scrapeTaskFunction(connectionString, proxyIP, r[0], UConvert.FromString<int>(r[1]), UConvert.FromString<int>(r[2])));
            }

            // Wait
            Task.WaitAll(tasks.ToArray());

            // Results
            foreach (Task<string> t in tasks) {
                CSA.Logger.LogInfo($"TASK COMPLETE: {t.Result}");
            }
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartReportScrapeTask(string connectionString, string proxyIP, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                ReportScraper scraper = new ReportScraper(connectionString, proxyIP, state, minimumID, maximumID);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }

        //--------------------------------------------------------------------------------
        static Task<string> StartPropertyScrapeTask(string connectionString, string proxyIP, string state, int minimumID, int maximumID) {
            return Task.Factory.StartNew(() => {
                PropertyScraper scraper = new PropertyScraper(connectionString, proxyIP, state, minimumID, maximumID);
                scraper.Scrape();
                return $"Thread {Thread.CurrentThread.ManagedThreadId}";
            });
        }
    }

}

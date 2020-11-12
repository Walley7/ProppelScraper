using CSACore.Utility;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Text;
using System.Threading;



namespace ProppelScraper.Scraping {

    public abstract class AddressScraper : Scraper {
        //================================================================================
        public const int                        MAXIMUM_SCRAPE_ATTEMPTS = 5;


        //================================================================================
        private string                          mURL;
        private string                          mState;
        private int                             mMinimumID;
        private int                             mMaximumID;
        private IDProvider                      mIDProvider;


        //================================================================================
        //--------------------------------------------------------------------------------
        public AddressScraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string url, string state, int minimumID, int maximumID) :
        base(connectionString, proxyIP, proxyUsername, proxyPassword) {
            mURL = url;
            mState = state;
            mMinimumID = minimumID;
            mMaximumID = maximumID;
        }

        //--------------------------------------------------------------------------------
        public AddressScraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string url, string state, IDProvider idProvider) :
        base(connectionString, proxyIP, proxyUsername, proxyPassword) {
            mURL = url;
            mState = state;
            mIDProvider = idProvider;
        }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        public override void Scrape() {
            // Open
            OpenLog();
            OpenDatabase();

            // Scrape - by range
            if (mIDProvider == null) {
                for (int i = mMinimumID; i <= mMaximumID; ++i) {
                    Scrape(i);
                }
            }

            // Scrape - by provider
            if (mIDProvider != null) {
                while (true) {
                    int? id = mIDProvider.NextID();
                    if (id == null)
                        break;
                    Scrape((int)id);
                }
            }

            // Close
            CloseDatabase();
            CloseLog();
        }

        //--------------------------------------------------------------------------------
        public void Scrape(int id) {
            // Scrape
            try {
                // Status
                DbCommand command = AddressData.CreateCommand("select status from Address where id = @ID", Connection);
                AddressData.AddParameter(command, "@ID", AddressID(id));
                string status = Program.RecordLookup ? (string)command.ExecuteScalar() ?? "" : "";
                command.Dispose();

                // Already processed
                if (AddressProcessed(status)) {
                    if (Program.VerboseLogging)
                        LogInfo(id, $"Already processed ('{status}')");
                    return;
                }

                // Address
                LogTrailingText = "";
                AddressData address;
                int attempts = 1;

                // Scrape
                while (true) {
                    address = ScrapeAddress(id);
                    if (address == null)
                        return;
                    if (address.status == AddressData.Status.SCRAPED || address.status == AddressData.Status.NOT_ADDRESS)
                        break;
                    else if (address.status == AddressData.Status.BLOCKED)
                        LogInfo(id, "BLOCKED BY WEBSITE");
                    if (attempts >= MAXIMUM_SCRAPE_ATTEMPTS) {
                        LogInfo(id, $"FAILED AFTER {MAXIMUM_SCRAPE_ATTEMPTS} ATTEMPTS");
                        return;
                    }
                    ++attempts;
                }

                // Update database
                LogInfo(id, address.StatusDisplayString + ".");
                address.Save(Connection);
            }
            catch (Exception e) {
                LogInfo(id, $"Exception \"{e.Message}\"");
            }
        }

        //--------------------------------------------------------------------------------
        public bool AddressProcessed(string status) {
            return (status == "scraped" || status == "not address");
        }
        
        //--------------------------------------------------------------------------------
        public abstract AddressData ScrapeAddress(int id);
        

        // TARGET ================================================================================
        //--------------------------------------------------------------------------------
        public string URL { get => mURL; }
        public string State { get => mState; }
        public int MinimumID { get => mMinimumID; }
        public int MaximumID { get => mMaximumID; }


        // ADDRESSES ================================================================================
        //--------------------------------------------------------------------------------
        public abstract string AddressID(int id);


        // LOGGING ================================================================================
        //--------------------------------------------------------------------------------
        public void LogInfo(int id, string text) { LogInfo($"{mState}-{id}: {text}"); }

        //--------------------------------------------------------------------------------
        protected override string LogPath {
            get {
                if (mIDProvider == null)
                    return Path.Combine(Program.LOGGING_PATH, $"Log ^ {UDateTime.Timestamp()} ^ {mState}-{mMinimumID}-{mMaximumID}.log");
                else
                    return Path.Combine(Program.LOGGING_PATH, $"Log ^ {UDateTime.Timestamp()} ^ {Thread.CurrentThread.ManagedThreadId} ^ {mState}-{mIDProvider.MinimumID}-{mIDProvider.MaximumID}.log");
            }
        }
    }

}

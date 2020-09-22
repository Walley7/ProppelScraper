using CSACore.Utility;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Text;



namespace ProppelScraper.Scraping {

    public abstract class AddressScraper : Scraper {
        //================================================================================
        private string                          mURL;
        private string                          mState;
        private int                             mMinimumID;
        private int                             mMaximumID;


        //================================================================================
        //--------------------------------------------------------------------------------
        public AddressScraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string url, string state, int minimumID, int maximumID) :
        base(connectionString, proxyIP, proxyUsername, proxyPassword) {
            mURL = url;
            mState = state;
            mMinimumID = minimumID;
            mMaximumID = maximumID;
        }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        public override void Scrape() {
            // Open
            OpenLog();
            OpenDatabase();

            // Scrape
            for (int i = mMinimumID; i <= mMaximumID; ++i) {
                Scrape(i);
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
                    LogInfo(id, $"Already processed ('{status}')");
                    return;
                }

                // Address
                LogTrailingText = "";
                AddressData address = ScrapeAddress(id);
                if (address == null)
                    return;
                if (address.status == AddressData.Status.BLOCKED) {
                    LogInfo(id, "BLOCKED BY WEBSITE");
                    return;
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
        protected override string LogFilename { get => $"Log ^ {UDateTime.Timestamp()} ^ {mState}-{mMinimumID}-{mMaximumID}.log"; }
    }

}

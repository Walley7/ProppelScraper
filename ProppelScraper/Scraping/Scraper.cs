﻿using CSACore.Logging;
using CSACore.Utility;
using HtmlAgilityPack;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;



namespace ProppelScraper.Scraping {

    public abstract class Scraper {
        //================================================================================
        public const int                        MAXIMUM_DOWNLOAD_ATTEMPTS = 5;
        public const int                        DOWNLOAD_TIMEOUT = 30000;


        //================================================================================
        private Logger                          mLogger;
        private string                          mLogTrailingText;

        private string                          mConnectionString;
        private DbConnection                    mConnection;

        private HtmlWeb                         mWeb = new HtmlWeb();
        private string                          mProxyIP;
        private string                          mProxyUsername;
        private string                          mProxyPassword;
        private int                             mDownloadAttempts;


        //================================================================================
        //--------------------------------------------------------------------------------
        public Scraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword) {
            // Input
            mConnectionString = connectionString;
            mProxyIP = proxyIP;
            mProxyUsername = proxyUsername;
            mProxyPassword = proxyPassword;

            // Timeout
            mWeb.PreRequest = delegate(HttpWebRequest webRequest) {
                webRequest.Timeout = DOWNLOAD_TIMEOUT;
                return true;
            };
        }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        public abstract void Scrape();


        // WEB ================================================================================
        //--------------------------------------------------------------------------------
        protected HtmlDocument DownloadURL(string url, int maximumAttempts = MAXIMUM_DOWNLOAD_ATTEMPTS) {
            // Attempts
            mDownloadAttempts = 1;

            // Download
            while (true) {
                try {
                    WebProxy proxy = (!string.IsNullOrWhiteSpace(mProxyIP) ? new WebProxy(mProxyIP) : null);
                    NetworkCredential credentials = new NetworkCredential(mProxyUsername, mProxyPassword);
                    HtmlDocument document = (proxy != null ? Web.Load(url, "GET", proxy, credentials) : Web.Load(url));
                    if ((int)Web.StatusCode < 200 || (int)Web.StatusCode >= 300)
                        throw new WebException($"Failed to download (Status {(int)Web.StatusCode}, {Web.StatusCode})");
                    return document;
                }
                catch (WebException e) {
                    if (mDownloadAttempts >= maximumAttempts)
                        throw e;
                    ++mDownloadAttempts;
                }
                catch (Exception e) { throw e; }
            }
        }

        //--------------------------------------------------------------------------------
        protected HtmlWeb Web { get => mWeb; }
        public string ProxyIP { get => mProxyIP; }
        protected int DownloadAttempts { get => mDownloadAttempts; }


        // DATABASE ================================================================================
        //--------------------------------------------------------------------------------
        public void OpenDatabase() {
            // Close
            CloseDatabase();

            // Connect
            if (Program.DatabaseIsMySQL)
                mConnection = new MySqlConnection(mConnectionString);
            else
                mConnection = new SQLiteConnection(mConnectionString);
            mConnection.Open();
            LogInfo($"Connected to database.");

            // Configuration
            //if (Program.DatabaseIsSQLite) {
            //    SQLiteCommand command = new SQLiteCommand("PRAGMA journal_mode=WAL;", (SQLiteConnection)mConnection);
            //    command.ExecuteNonQuery();
            //    command.Dispose();
            //}
        }
        
        //--------------------------------------------------------------------------------
        public void CloseDatabase() {
            if (mConnection != null)
                mConnection.Close();
        }

        //--------------------------------------------------------------------------------
        public DbConnection Connection { get => mConnection; }


        // LOGGING ================================================================================
        //--------------------------------------------------------------------------------
        public void OpenLog() {
            // Close
            CloseLog();

            // Open
            mLogger = new Logger(UFile.IncrementalFreePath(LogPath));
            LogStart();
        }
        
        //--------------------------------------------------------------------------------
        public void CloseLog() {
            if (mLogger != null) {
                LogEnd();
                mLogger.Dispose();
                mLogger = null;
            }
        }

        //--------------------------------------------------------------------------------
        private void LogStart() {
            mLogger.LogToConsole = false;
            mLogger.LogStart();
            mLogger.LogToConsole = true;
        }

        //--------------------------------------------------------------------------------
        private void LogEnd() {
            mLogger.LogToConsole = false;
            mLogger.LogEnd();
            mLogger.LogToConsole = true;
        }

        //--------------------------------------------------------------------------------
        public void LogInfo(string text) {
            mLogger.LogInfo($"(THREAD {Thread.CurrentThread.ManagedThreadId}) {text}{mLogTrailingText}");
            mLogTrailingText = "";
        }

        //--------------------------------------------------------------------------------
        public Logger Logger { get => mLogger; }
        protected virtual string LogPath { get => Path.Combine(Program.LOGGING_PATH, $"Log ^ {UDateTime.Timestamp()}.log"); }
        public string LogTrailingText { set => mLogTrailingText = value; get => mLogTrailingText; }
    }
}

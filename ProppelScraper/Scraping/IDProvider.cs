using System;
using System.Collections.Generic;
using System.Text;



namespace ProppelScraper.Scraping {

    public class IDProvider {
        //================================================================================
        private int                             mMinimumID;
        private int                             mMaximumID;
        private int                             mNextID;

        private object                          mLock = new object();


        //================================================================================
        //--------------------------------------------------------------------------------
        public IDProvider(int minimumID, int maximumID) {
            mMinimumID = minimumID;
            mMaximumID = maximumID;
            mNextID = mMinimumID;
        }


        // ID GENERATION ================================================================================
        //--------------------------------------------------------------------------------
        public int? NextID() {
            lock (mLock) {
                if (mNextID > mMaximumID)
                    return null;
                return mNextID++;
            }
        }

        //--------------------------------------------------------------------------------
        public int MinimumID { get => mMinimumID; }
        public int MaximumID { get => mMaximumID; }
    }

}

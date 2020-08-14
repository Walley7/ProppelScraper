using CSACore.Scraping;
using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;



namespace ProppelScraper.Scraping {

    public class ReportScraper : AddressScraper {
        //================================================================================
        public const string                     BASE_URL = "http://house.ksou.cn/report.php?q=_";


        //================================================================================
        //--------------------------------------------------------------------------------
        public ReportScraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID = -1, int maximumID = -1) :
        base(connectionString, proxyIP, proxyUsername, proxyPassword, BASE_URL, state, minimumID, maximumID) { }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        public override AddressData ScrapeAddress(int id) {
            // Scrape
            try {
                // Download
                HtmlDocument document = DownloadURL($"{URL}&sta={State}&askid={id}");
                string html = document.DocumentNode.InnerHtml;

                // Log - multiple download attempts
                if (DownloadAttempts > 1)
                    LogTrailingText = $"  [Downloaded after {DownloadAttempts} attempts]";
                //LogInfo(id, $"Downloaded after {DownloadAttempts} attempts");

                // Checks
                if (html.Contains("<td>Your request is under process, please wait for <b>"))
                    return new AddressData(AddressData.Status.BLOCKED);
                else if (string.IsNullOrWhiteSpace(html) || !html.Contains("<nobr>Median Price</nobr>"))
                    return new AddressData(AddressID(id), "report", AddressData.Status.NOT_ADDRESS);

                // Trim comparable houses (causes section identification problems otherwise)
                int comparableHousesIndex = html.IndexOf("<b>Comparable Houses Recent Sold</b>");

                // Scraper / address
                StringScraper scraper = new StringScraper(comparableHousesIndex != -1 ? html.Substring(0, comparableHousesIndex) : html);
                AddressData address = new AddressData(AddressID(id), "report", AddressData.Status.SCRAPED);

                // Property section
                address.address = scraper.ReadPastAndTo("<table cellspacing=\"10\" style=\"font-size:18px;color:#261cdc\"><tr><td><b>", "</b></td></tr></table>");

                if (scraper.ReadToCheck("<td><b>Unit:</b></td>") || scraper.ReadToCheck("<td><b>House:</b></td>") || scraper.ReadToCheck("<td><b>Townhouse:</b></td>")) {
                    address.type = scraper.ReadPastAndTo("<td><b>", ":</b></td>");
                    scraper.ReadPast(":</b></td><td>");
                    address.bedrooms = scraper.ReadToAndSkip(" <img src=\"/img/bed.png\" border=\"0\" alt=\"Bed rooms\" title=\"Bed rooms\"> ");
                    address.bathrooms = scraper.ReadToAndSkip(" <img src=\"/img/bath.png\" border=\"0\" alt=\"Bath rooms\" title=\"Bath rooms\"> ");
                    address.carSpaces = scraper.ReadToAndSkip(" <img src=\"/img/car.png\" border=\"0\" alt=\"Car spaces\" title=\"Car spaces\">");
                }
                else if (scraper.ReadToCheck("<td><b>Land:</b>"))
                    address.type = "Land";

                address.landSize = scraper.ReadPastAndTo("<td><b>Land size:</b></td><td>", "&nbsp;<a href=\"measure.php");
                address.soldFor = scraper.ReadPastAndTo("title=\"Click to view more about property sales information\">", " in ");
                address.soldOn = scraper.ReadPastAndTo(" in ", "</a></td></tr>");
                address.estimatedLowerValue = scraper.ReadPastAndTo("<tr><td><b>Estimate:</b></td><td>", " - ");
                address.estimatedUpperValue = scraper.ReadPastAndTo(" - ", "</td></tr>");
                address.buildYear = scraper.ReadPastAndTo("<td><b>Build year:</b></td><td>", "</td></tr>");
                //address.distances = scraper.ReadPastAndToOrTo("<td><b>Distance:</b> ", "&nbsp;<a href=", "</td></tr>"); // Can't get this - it's auto-generated from a span somehow

                // Property section (images)
                if (scraper.ReadToCheck("</div><a href=\"http://house.ksou.cn/house_img.php?")) {
                    address.thumbnailURL = scraper.ReadPastAndPast("title=\"Click to view more photos\"><img src=\"", ".jpg");
                    address.imagesURL = scraper.ReadToAndTo("http://house.ksou.cn/house_img.php?", "\" target=\"_blank\"");
                }

                address.neighbourImagesURL = scraper.ReadToAndTo("http://house.ksou.cn/neighbour_img.php?", "\" target=\"_blank\"");
                address.floorPlanURL = scraper.ReadToAndTo("http://house.ksou.cn/floorplan.php?", "\" target=\"_blank\" style=\"font-size:12px\">Floorplan");

                // Return
                return address;
            }
            catch (Exception e) {
                LogInfo(id, $"Exception \"{e.Message}\"");
            }

            // Failure
            return null;
        }


        // ADDRESSES ================================================================================
        //--------------------------------------------------------------------------------
        public override string AddressID(int id) { return $"r{State}{id}"; }
    }

}

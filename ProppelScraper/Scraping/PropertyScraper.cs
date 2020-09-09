using CSACore.Logging;
using CSACore.Scraping;
using CSACore.Utility;
using HtmlAgilityPack;
using ScrapySharp.Extensions;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;



namespace ProppelScraper.Scraping {

    public class PropertyScraper : AddressScraper {
        //================================================================================
        public const string                     BASE_URL = "http://house.ksou.cn/p.php?q=_";


        //================================================================================
        //--------------------------------------------------------------------------------
        public PropertyScraper(string connectionString, string proxyIP, string proxyUsername, string proxyPassword, string state, int minimumID = -1, int maximumID = -1) :
        base(connectionString, proxyIP, proxyUsername, proxyPassword, BASE_URL, state, minimumID, maximumID) { }


        // SCRAPING ================================================================================
        //--------------------------------------------------------------------------------
        public override AddressData ScrapeAddress(int id) {
            // Scrape
            try {
                // Download
                HtmlDocument document = DownloadURL($"{URL}&sta={State}&id={id}");
                string html = document.DocumentNode.InnerHtml;

                // Log - multiple download attempts
                if (DownloadAttempts > 1)
                    LogTrailingText = $"  [Downloaded after {DownloadAttempts} attempts]";
                //LogInfo(id, $"Downloaded after {DownloadAttempts} attempts");

                // Checks
                if (string.IsNullOrWhiteSpace(html) || html.Contains("<td>Your request is under process, please wait for <b>"))
                    return new AddressData(AddressData.Status.BLOCKED);
                else if (!html.Contains("<nobr>Median Price</nobr>"))
                    return new AddressData(AddressID(id), "property", AddressData.Status.NOT_ADDRESS);

                // Scraper / address
                StringScraper scraper = new StringScraper(html);
                AddressData address = new AddressData(AddressID(id), "property", AddressData.Status.SCRAPED);

                // Property section (upper)
                address.address = scraper.ReadPastAndTo("<span class=\"addr\">", "</span></td>");

                if (scraper.ReadPastCheck("<td><b>Sold ")) {
                    address.soldFor = scraper.ReadTo("</b>");
                    address.soldOn = scraper.ReadPastAndTo("</b> in ", "&nbsp;<a href=");
                }

                scraper.ReadPast("\" target=\"_blank\">Days on Market</a>");
                scraper.ReadPast("<td><b>Last Sold</b>");

                if (scraper.ReadPastCheck("<td><b>Rent</b>")) {
                    //address.rent = scraper.ReadPastAndTo("target=\"_blank\">", " in ");
                    address.rent = scraper.ReadToAndTo("$", " in ");
                    //address.rentOn = scraper.ReadPastAndTo(" in ", "</a></td>");
                    address.rentOn = scraper.ReadPastAndTo(" in ", "</td>").Replace("</a>", "");
                }

                scraper.ReadPast("<td>");
                if (scraper.RemainingString.StartsWith("<b>")) {
                    address.type = scraper.ReadPastAndTo("<b>", "</b>").Replace(":", "");
                    scraper.ReadPast("</b>");
                }
                address.bedrooms = scraper.ReadToAndSkip(" <img src=\"/img/bed.png\" border=\"0\" alt=\"Bed rooms\" title=\"Bed rooms\"> ").Replace(" ", "");
                address.bathrooms = scraper.ReadToAndSkip(" <img src=\"/img/bath.png\" border=\"0\" alt=\"Bath rooms\" title=\"Bath rooms\"> ").Replace(" ", "");
                address.carSpaces = scraper.ReadToAndSkip(" <img src=\"/img/car.png\" border=\"0\" alt=\"Car spaces\" title=\"Car spaces\">").Replace(" ", "");
                
                //address.landSize = scraper.ReadPastAndToOrTo("<td><b>Land size:</b> ", " | <b>Building size:</b>", "&nbsp;<a href=\"measure.php");
                address.landSize = scraper.ReadPastAndPast("<td><b>Land size:</b> ", " sqm");
                //address.buildingSize = scraper.ReadPastAndTo("Building size:</b> ", "&nbsp;<a href=\"measure.php");
                address.buildingSize = scraper.ReadPastAndPast("<b>Building size:</b> ", " sqm");

                address.buildYear = scraper.ReadPastAndTo("<td><b>Build year:</b> ", "</td>");
                address.agent = scraper.ReadPastAndTo("<td><b>Agent:</b> ", "</td>");
                address.distances = scraper.ReadPastAndToOrTo("<td><b>Distance:</b> ", "&nbsp;<a href=", "</td></tr>");

                // Property section (images)
                address.thumbnailURL = scraper.ReadPastAndPast("title=\"Click to view more photos\"><img src=\"", $"{id}.jpg");
                address.imagesURL = scraper.ReadToAndTo("http://house.speakingsame.com/house_img.php?", "\" target=\"_blank\"");
                address.neighbourImagesURL = scraper.ReadToAndTo("http://house.speakingsame.com/neighbour_img.php?", "\" target=\"_blank\"");

                // Property section (footer)
                address.floorPlanURL = scraper.ReadToAndTo("http://house.speakingsame.com/floorplan.php?", "\" target=\"_blank\">Floorplan");

                // Description section
                if (scraper.ReadToCheck("Lot/Plan No: ")) {
                    address.lot = scraper.ReadPastAndTo("Lot/Plan No: ", "/");
                    address.planNumber = scraper.ReadPastAndTo("/", "</div></td>");
                }

                address.propertyZoneURL = scraper.ReadToAndTo("http://www.showneighbour.com/propertyzone.php?", "\" target=\"_blank\">Property's zone");

                if (scraper.ReadToCheck("target=\"_blank\">Estimate ")) {
                    address.estimatedLowerValue = scraper.ReadPastAndTo("target=\"_blank\">Estimate ", " - ");
                    address.estimatedUpperValue = scraper.ReadPastAndTo(" - ", ", view property report</a></td>");
                }

                // Schools section
                if (scraper.ReadPastCheck("<td><b>Nearby Schools:</b></td>")) {
                    while (scraper.ReadToCheck("\"school.php?")) {
                        AddressData.SchoolData school = new AddressData.SchoolData();
                        address.schools.Add(school);
                        school.url = "http://house.speakingsame.com/" + scraper.ReadPastAndTo("\"", "\">");
                        school.CaptureID();
                        school.name = scraper.ReadPastAndTo("\">", "</a></td>");
                        school.type = scraper.ReadPastAndTo("<td>", "</td>");
                        school.rank = scraper.ReadPastAndTo("<td class=\"sm\">", "</td>");
                        school.distance = scraper.ReadPastAndTo("<td>", "</td>");
                    }
                }

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
        public override string AddressID(int id) { return $"p{State}{id}"; }
    }

}

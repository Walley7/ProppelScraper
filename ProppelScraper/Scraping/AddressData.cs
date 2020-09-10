using CSACore.Core;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Text;



namespace ProppelScraper.Scraping {

    public class AddressData {
        // This widget is the root of your application.
        //================================================================================
        public enum Status {
            SCRAPED,
            NOT_ADDRESS,
            BLOCKED
        }


        //================================================================================
        public string                           id;
        public string                           source;
        public Status                           status;

        public string                           address;
        public string                           soldFor;
        public string                           soldOn;
        public string                           rent;
        public string                           rentOn;
        public string                           type;
        public string                           bedrooms;
        public string                           bathrooms;
        public string                           carSpaces;
        public string                           landSize;
        public string                           buildingSize;
        public string                           buildYear;
        public string                           agent;
        public string                           distances;
        public string                           thumbnailURL;
        public string                           imagesURL;
        public string                           neighbourImagesURL;
        public string                           floorPlanURL;
        public string                           lot;
        public string                           planNumber;
        public string                           propertyZoneURL;
        public string                           estimatedLowerValue;
        public string                           estimatedUpperValue;

        public List<SchoolData>                 schools = new List<SchoolData>();


        //================================================================================
        //--------------------------------------------------------------------------------
        public AddressData(string id, string source, Status status) {
            this.id = id;
            this.source = source;
            this.status = status;
        }

        //--------------------------------------------------------------------------------
        public AddressData(Status status) : this("", "", status) { }


        // STATUS ================================================================================
        //--------------------------------------------------------------------------------
        public string StatusString {
            get {
                switch (status) {
                    case Status.SCRAPED:     return "scraped";
                    case Status.NOT_ADDRESS:        return "not address";
                    default:                        return "";
                }
            }
        }

        //--------------------------------------------------------------------------------
        public string StatusDisplayString {
            get {
                switch (status) {
                    case Status.SCRAPED:     return "Scraped";
                    case Status.NOT_ADDRESS:        return "Not an address";
                    default:                        return "";
                }
            }
        }


        // DATABASE ================================================================================
        //--------------------------------------------------------------------------------
        public static void InitialiseDatabase(SQLiteConnection connection) {
            // Address table
            using (SQLiteCommand command = new SQLiteCommand("select name from sqlite_master where type = 'table' and name = 'Address'", connection)) {
                if (command.ExecuteScalar() == null) {
                    // Query
                    SQLiteCommand tableCommand = new SQLiteCommand(
                        "create table Address (" +
                        "  id varchar(32)" + (Program.DatabaseMode == Program.EDatabaseMode.INDEXED ? " primary key" : "") + ", " +
                        "  source varchar(32), " +
                        "  status varchar(32), " +
                        "  address varchar(256), " +
                        "  sold_for varchar(32), " +
                        "  sold_on varchar(32), " +
                        "  rent varchar(32), " +
                        "  rent_on varchar(32), " +
                        "  type varchar(128), " +
                        "  bedrooms varchar(8), " +
                        "  bathrooms varchar(8), " +
                        "  car_spaces varchar(8), " +
                        "  land_size varchar(32), " +
                        "  building_size varchar(32), " +
                        "  build_year varchar(8), " +
                        "  agent varchar(256), " +
                        "  distances varchar(1024), " +
                        "  thumbnail_url varchar(256), " +
                        "  images_url varchar(256), " +
                        "  neighbour_images_url varchar(256), " +
                        "  floor_plan_url varchar(256), " +
                        "  lot varchar(32), " +
                        "  plan_number varchar(32), " +
                        "  property_zone_url varchar(256), " +
                        "  estimated_lower_value varchar(32), " +
                        "  estimated_upper_value varchar(32) " +
                        ");",
                        connection
                    );

                    // Execute
                    tableCommand.ExecuteNonQuery();
                    tableCommand.Dispose();
                    CSA.Logger.LogInfo("Created address table.");
                }
            }

            using (SQLiteCommand command = new SQLiteCommand("select name from sqlite_master where type = 'table' and name = 'AddressSchool'", connection)) {
                if (command.ExecuteScalar() == null) {
                    // Query
                    SQLiteCommand tableCommand = new SQLiteCommand(
                        "create table AddressSchool (" +
                        "  id integer" + (Program.DatabaseMode == Program.EDatabaseMode.INDEXED ? " primary key" : "") + ", " +
                        "  address_id varchar(32) references Address(id), " +
                        "  school_id varchar(32), " +
                        "  type varchar(32), " +
                        "  rank varchar(32), " +
                        "  distance varchar(8), " +
                        "  url varchar(256) " +
                        ");",
                        connection    
                    );

                    // Execute
                    tableCommand.ExecuteNonQuery();
                    tableCommand.Dispose();
                    CSA.Logger.LogInfo("Created address school table.");
                };
            }
        }

        //--------------------------------------------------------------------------------
        public bool Save(SQLiteConnection connection) {
            // Address - ID
            SQLiteCommand command = new SQLiteCommand("select id from Address where id = @ID", connection);
            command.Parameters.AddWithValue("@ID", id);
            object addressID = Program.DatabaseMode == Program.EDatabaseMode.INDEXED ? command.ExecuteScalar() : null;
            command.Dispose();

            // Address - query
            if (addressID == null) {
                command = new SQLiteCommand("insert into Address (id, source, status, address, sold_for, sold_on, rent, rent_on, type, bedrooms, bathrooms, car_spaces, " +
                                            "  land_size, building_size, build_year, agent, distances, thumbnail_url, images_url, neighbour_images_url, floor_plan_url, " +
                                            "  lot, plan_number, property_zone_url, estimated_lower_value, estimated_upper_value) " +
                                            "values (@ID, @Source, @Status, @Address, @SoldFor, @SoldOn, @Rent, @RentOn, @Type, @Bedrooms, @Bathrooms, @CarSpaces, " +
                                            "        @LandSize, @BuildingSize, @BuildYear, @Agent, @Distances, @ThumbnailURL, @ImagesURL, @NeighbourImagesURL, " +
                                            "        @FloorPlanURL, @Lot, @PlanNumber, @PropertyZoneURL, @EstimatedLowerValue, @EstimatedUpperValue)",
                                            connection);
            }
            else {
                command = new SQLiteCommand("update Address set source = @Source, status = @Status, address = @Address, sold_for = @SoldFor, sold_on = @SoldOn, rent = @Rent, " +
                                            "  rent_on = @RentOn, type = @Type, bedrooms = @Bedrooms, bathrooms = @Bathrooms, car_spaces = @CarSpaces, " +
                                            "  land_size = @LandSize, building_size = @BuildingSize, build_year = @BuildYear, agent = @Agent, distances = @Distances, " +
                                            "  thumbnail_url = @ThumbnailURL, images_url = @ImagesURL, neighbour_images_url = @NeighbourImagesURL, " +
                                            "  floor_plan_url = @FloorPlanURL, lot = @Lot, plan_number = @PlanNumber, property_zone_url = @PropertyZoneURL, " +
                                            "  estimated_lower_value = @EstimatedLowerValue, estimated_upper_value = @EstimatedUpperValue " +
                                            "where id = @ID",
                                            connection);
            }

            // Address - parameters
            command.Parameters.AddWithValue("@ID", id);
            command.Parameters.AddWithValue("@Source", source);
            command.Parameters.AddWithValue("@Status", StatusString);
            command.Parameters.AddWithValue("@Address", address);
            command.Parameters.AddWithValue("@SoldFor", soldFor);
            command.Parameters.AddWithValue("@SoldOn", soldOn);
            command.Parameters.AddWithValue("@Rent", rent);
            command.Parameters.AddWithValue("@RentOn", rentOn);
            command.Parameters.AddWithValue("@Type", type);
            command.Parameters.AddWithValue("@Bedrooms", bedrooms);
            command.Parameters.AddWithValue("@Bathrooms", bathrooms);
            command.Parameters.AddWithValue("@CarSpaces", carSpaces);
            command.Parameters.AddWithValue("@LandSize", landSize);
            command.Parameters.AddWithValue("@BuildingSize", buildingSize);
            command.Parameters.AddWithValue("@BuildYear", buildYear);
            command.Parameters.AddWithValue("@Agent", agent);
            command.Parameters.AddWithValue("@Distances", distances);
            command.Parameters.AddWithValue("@ThumbnailURL", thumbnailURL);
            command.Parameters.AddWithValue("@ImagesURL", imagesURL);
            command.Parameters.AddWithValue("@NeighbourImagesURL", neighbourImagesURL);
            command.Parameters.AddWithValue("@FloorPlanURL", floorPlanURL);
            command.Parameters.AddWithValue("@Lot", lot);
            command.Parameters.AddWithValue("@PlanNumber", planNumber);
            command.Parameters.AddWithValue("@PropertyZoneURL", propertyZoneURL);
            command.Parameters.AddWithValue("@EstimatedLowerValue", estimatedLowerValue);
            command.Parameters.AddWithValue("@EstimatedUpperValue", estimatedUpperValue);

            // Address - execute
            command.ExecuteNonQuery();
            command.Dispose();
            
            // Schools
            foreach (SchoolData s in schools) {
                // ID
                command = new SQLiteCommand("select id from AddressSchool where address_id = @ID and school_id = @SchoolID", connection);
                command.Parameters.AddWithValue("@ID", id);
                command.Parameters.AddWithValue("@SchoolID", s.id);
                object schoolID = Program.DatabaseMode == Program.EDatabaseMode.INDEXED ? command.ExecuteScalar() : null;
                command.Dispose();

                // Query
                if (schoolID == null) {
                    command = new SQLiteCommand("insert into AddressSchool (address_id, school_id, type, rank, distance, url) " +
                                                "values (@ID, @SchoolID, @Type, @Rank, @Distance, @URL)", connection);
                }
                else {
                    command = new SQLiteCommand("update AddressSchool set address_id = @ID, school_id = @SchoolID, type = @Type, rank = @Rank, distance = @Distance, url = @URL " +
                                                "where address_id = @ID and school_id = @SchoolID", connection);
                }

                // Parameters
                command.Parameters.AddWithValue("@ID", id);
                command.Parameters.AddWithValue("@SchoolID", s.id);
                command.Parameters.AddWithValue("@Type", s.type);
                command.Parameters.AddWithValue("@Rank", s.rank);
                command.Parameters.AddWithValue("@Distance", s.distance);
                command.Parameters.AddWithValue("@URL", s.url);

                // Execute
                command.ExecuteNonQuery();
                command.Dispose();
            }

            // Return
            return (addressID == null);
        }


        // STRINGS ================================================================================
        //--------------------------------------------------------------------------------
        public override string ToString() {
            StringBuilder builder = new StringBuilder();
            builder.AppendLine($"Address:                {address}");
            builder.AppendLine($"Sold For:               {soldFor}");
            builder.AppendLine($"Sold On:                {soldOn}");
            builder.AppendLine($"Rent:                   {rent}");
            builder.AppendLine($"Rent On:                {rentOn}");
            builder.AppendLine($"Type:                   {type}");
            builder.AppendLine($"Bedrooms:               {bedrooms}");
            builder.AppendLine($"Bathrooms:              {bathrooms}");
            builder.AppendLine($"Car Spaces:             {carSpaces}");
            builder.AppendLine($"Land Size:              {landSize}");
            builder.AppendLine($"Building Size:          {buildingSize}");
            builder.AppendLine($"Build Year:             {buildYear}");
            builder.AppendLine($"Agent:                  {agent}");
            builder.AppendLine($"Distances:              {distances}");
            builder.AppendLine($"Thumbnail URL:          {thumbnailURL}");
            builder.AppendLine($"Images URL:             {imagesURL}");
            builder.AppendLine($"Neighbour Images URL:   {neighbourImagesURL}");
            builder.AppendLine($"Floor Plan URL:         {floorPlanURL}");
            builder.AppendLine($"Lot:                    {lot}");
            builder.AppendLine($"Plan Number:            {planNumber}");
            builder.AppendLine($"Property Zone URL:      {propertyZoneURL}");
            builder.AppendLine($"Estimated Lower Value:  {estimatedLowerValue}");
            builder.AppendLine($"Estimated Upper Value:  {estimatedUpperValue}");

            for (int i = 0; i < schools.Count; ++i) {
                builder.AppendLine($"School {i + 1}:               {schools[i].name}");
                builder.AppendLine($"School {i + 1} - ID:          {schools[i].id}");
                builder.AppendLine($"School {i + 1} - Type:        {schools[i].type}");
                builder.AppendLine($"School {i + 1} - Rank:        {schools[i].rank}");
                builder.AppendLine($"School {i + 1} - Distance:    {schools[i].distance}");
                builder.AppendLine($"School {i + 1} - URL:         {schools[i].url}");
            }

            return builder.ToString();
        }


        //================================================================================
        //********************************************************************************
        public class SchoolData {
            public string id;
            public string name;
            public string type;
            public string rank;
            public string distance;
            public string url;

            public bool CaptureID() {
                int startIndex = url.IndexOf("?id=");
                if (startIndex == -1)
                    return false;
                startIndex += 4;
                int endIndex = url.IndexOf("&", startIndex);
                if (endIndex == -1)
                    endIndex = url.Length;
                id = url.Substring(startIndex, endIndex - startIndex);
                return true;
            }
        }
    }

}

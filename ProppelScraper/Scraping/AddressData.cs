using CSACore.Core;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
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
        public static void InitialiseDatabase(DbConnection connection) {
            // Address table
            using (DbCommand command = CreateCommand($"select table_name from information_schema.tables where table_schema = '{connection.Database}' and table_name = 'Address'",
                                                     "select name from sqlite_master where type = 'table' and name = 'Address'", connection))
            {
                if (command.ExecuteScalar() == null) {
                    // Query
                    DbCommand tableCommand = CreateCommand(
                        "create table Address (" +
                        "  id varchar(32)" + (Program.DatabaseIsMySQL ? " not null" : "") + ", " +
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

            // Address school table
            using (DbCommand command = CreateCommand($"select table_name from information_schema.tables where table_schema = '{connection.Database}' and table_name = 'AddressSchool'",
                                                     "select name from sqlite_master where type = 'table' and name = 'AddressSchool'", connection))
            {
                if (command.ExecuteScalar() == null) {
                    // Query
                    DbCommand tableCommand = CreateCommand(
                        "create table AddressSchool (" +
                        "  address_id varchar(32) references Address(id), " +
                        "  school_id varchar(32), " +
                        "  name varchar(256), " +
                        "  type varchar(32), " +
                        "  rank varchar(32), " +
                        "  distance varchar(128), " +
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

            // Indexing - address
            if (Program.RecordLookup) {
                using (DbCommand command = CreateCommand($"select table_name from information_schema.statistics where table_schema = '{connection.Database}' and table_name = 'Address' and index_name = 'PRIMARY'",
                                                         "", connection))
                {
                    if (command.ExecuteScalar() == null) {
                        DbCommand tableCommand = CreateCommand("alter table Address add primary key (id)", "", connection);
                        tableCommand.ExecuteNonQuery();
                        tableCommand.Dispose();
                        CSA.Logger.LogInfo("Added primary key to address table.");
                    }
                }
            }

            // Indexing - address school
            if (Program.RecordLookup) {
                using (DbCommand command = CreateCommand($"select table_name from information_schema.statistics where table_schema = '{connection.Database}' and table_name = 'AddressSchool' and index_name = 'PRIMARY'",
                                                         "", connection))
                {
                    if (command.ExecuteScalar() == null) {
                        DbCommand tableCommand = CreateCommand("alter table AddressSchool add id integer not null auto_increment primary key", "", connection);
                        tableCommand.ExecuteNonQuery();
                        tableCommand.Dispose();
                        CSA.Logger.LogInfo("Added primary key to address school table.");
                    }
                }
            }
        }

        //--------------------------------------------------------------------------------
        public bool Save(DbConnection connection) {
            // Address - ID
            DbCommand command = CreateCommand("select id from Address where id = @ID", connection);
            AddParameter(command, "@ID", id);
            object addressID = Program.RecordLookup ? command.ExecuteScalar() : null;
            command.Dispose();

            // Address - query
            if (addressID == null) {
                command = CreateCommand("insert into Address (id, source, status, address, sold_for, sold_on, rent, rent_on, type, bedrooms, bathrooms, car_spaces, " +
                                        "  land_size, building_size, build_year, agent, distances, thumbnail_url, images_url, neighbour_images_url, floor_plan_url, " +
                                        "  lot, plan_number, property_zone_url, estimated_lower_value, estimated_upper_value) " +
                                        "values (@ID, @Source, @Status, @Address, @SoldFor, @SoldOn, @Rent, @RentOn, @Type, @Bedrooms, @Bathrooms, @CarSpaces, " +
                                        "        @LandSize, @BuildingSize, @BuildYear, @Agent, @Distances, @ThumbnailURL, @ImagesURL, @NeighbourImagesURL, " +
                                        "        @FloorPlanURL, @Lot, @PlanNumber, @PropertyZoneURL, @EstimatedLowerValue, @EstimatedUpperValue)",
                                        connection);
            }
            else {
                command = CreateCommand("update Address set source = @Source, status = @Status, address = @Address, sold_for = @SoldFor, sold_on = @SoldOn, rent = @Rent, " +
                                        "  rent_on = @RentOn, type = @Type, bedrooms = @Bedrooms, bathrooms = @Bathrooms, car_spaces = @CarSpaces, " +
                                        "  land_size = @LandSize, building_size = @BuildingSize, build_year = @BuildYear, agent = @Agent, distances = @Distances, " +
                                        "  thumbnail_url = @ThumbnailURL, images_url = @ImagesURL, neighbour_images_url = @NeighbourImagesURL, " +
                                        "  floor_plan_url = @FloorPlanURL, lot = @Lot, plan_number = @PlanNumber, property_zone_url = @PropertyZoneURL, " +
                                        "  estimated_lower_value = @EstimatedLowerValue, estimated_upper_value = @EstimatedUpperValue " +
                                        "where id = @ID",
                                        connection);
            }

            // Address - parameters
            AddParameter(command, "@ID", id);
            AddParameter(command, "@Source", source);
            AddParameter(command, "@Status", StatusString);
            AddParameter(command, "@Address", address);
            AddParameter(command, "@SoldFor", soldFor);
            AddParameter(command, "@SoldOn", soldOn);
            AddParameter(command, "@Rent", rent);
            AddParameter(command, "@RentOn", rentOn);
            AddParameter(command, "@Type", type);
            AddParameter(command, "@Bedrooms", bedrooms);
            AddParameter(command, "@Bathrooms", bathrooms);
            AddParameter(command, "@CarSpaces", carSpaces);
            AddParameter(command, "@LandSize", landSize);
            AddParameter(command, "@BuildingSize", buildingSize);
            AddParameter(command, "@BuildYear", buildYear);
            AddParameter(command, "@Agent", agent);
            AddParameter(command, "@Distances", distances);
            AddParameter(command, "@ThumbnailURL", thumbnailURL);
            AddParameter(command, "@ImagesURL", imagesURL);
            AddParameter(command, "@NeighbourImagesURL", neighbourImagesURL);
            AddParameter(command, "@FloorPlanURL", floorPlanURL);
            AddParameter(command, "@Lot", lot);
            AddParameter(command, "@PlanNumber", planNumber);
            AddParameter(command, "@PropertyZoneURL", propertyZoneURL);
            AddParameter(command, "@EstimatedLowerValue", estimatedLowerValue);
            AddParameter(command, "@EstimatedUpperValue", estimatedUpperValue);

            // Address - execute
            command.ExecuteNonQuery();
            command.Dispose();
            
            // Schools
            foreach (SchoolData s in schools) {
                // ID
                command = CreateCommand("select id from AddressSchool where address_id = @ID and school_id = @SchoolID", connection);
                AddParameter(command, "@ID", id);
                AddParameter(command, "@SchoolID", s.id);
                object schoolID = Program.RecordLookup ? command.ExecuteScalar() : null;
                command.Dispose();

                // Query
                if (schoolID == null) {
                    command = CreateCommand("insert into AddressSchool (address_id, school_id, name, type, rank, distance, url) " +
                                            "values (@ID, @SchoolID, @Name, @Type, @Rank, @Distance, @URL)", connection);
                }
                else {
                    command = CreateCommand("update AddressSchool set address_id = @ID, school_id = @SchoolID, name = @Name, type = @Type, rank = @Rank, distance = @Distance, url = @URL " +
                                            "where address_id = @ID and school_id = @SchoolID", connection);
                }

                // Parameters
                AddParameter(command, "@ID", id);
                AddParameter(command, "@SchoolID", s.id);
                AddParameter(command, "@Name", s.name);
                AddParameter(command, "@Type", s.type);
                AddParameter(command, "@Rank", s.rank);
                AddParameter(command, "@Distance", s.distance);
                AddParameter(command, "@URL", s.url);

                // Execute
                command.ExecuteNonQuery();
                command.Dispose();
            }

            // Return
            return (addressID == null);
        }

        //--------------------------------------------------------------------------------
        public static DbCommand CreateCommand(string commandText, DbConnection connection) {
            if (Program.DatabaseIsMySQL)
                return new MySqlCommand(commandText, (MySqlConnection)connection);
            else
                return new SQLiteCommand(commandText, (SQLiteConnection)connection);
        }

        //--------------------------------------------------------------------------------
        public static DbCommand CreateCommand(string mysqlCommandText, string sqliteCommandText, DbConnection connection) {
            return CreateCommand(Program.DatabaseIsMySQL ? mysqlCommandText : sqliteCommandText, connection);
        }

        //--------------------------------------------------------------------------------
        public static DbCommand AddParameter(DbCommand command, string parameterName, object value) {
            DbParameter parameter = command.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.Value = value;
            command.Parameters.Add(parameter);
            return command;
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

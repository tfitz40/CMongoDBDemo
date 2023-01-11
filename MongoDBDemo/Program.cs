using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDBDemo
{
    class Program
    {
        static MongoAccess db = new MongoAccess("HelloWorld");
        
        static void Main(string[] args)
        {
            ListDatabases();
            //InsertRecord();
            //ReadRecord();
            //ReadDocumentWithFilter("_id", "632b5a7beff54cdc803a80bd");
            //Sort("_id");
            //Project("_id");
            //TruncateCollection();

            //var users = LoadRecords<PersonModel>("Users");
            //users.ToConsole();

            //var user = LoadRecordById("Users", new Guid("0866828b-2d01-458d-b2fd-8bc2a1d011fe"));
            //user.ToConsole();
            //user.DateOfBirth = new DateTime(1990, 12, 21, 0, 0, 0, DateTimeKind.Utc);
            //user.ToConsole();
            //db.UpsertRecord("Users", user.Id, user);

            //db.DeleteRecords<PersonModel>("Users", user.Id);

            //InsertRecord("Users", new PersonModel
            //{
            //    FirstName = "Trevor",
            //    LastName = "Fitzsimmons",
            //    PrimaryAddress = new AddressModel
            //    {
            //        StreetAddress = "101 Oak Street",
            //        City = "Scranton",
            //        State = "PA",
            //        ZipCode = "18512"
            //    }
            //});



            //var users = LoadRecords<PersonModel>("users");
            //var users = new List<PersonModel>
            //{
            //    new PersonModel { FirstName = "Joe", LastName = "Smith" },
            //    new PersonModel { FirstName = "Jane", LastName = "Smith", PrimaryAddress = new AddressModel { StreetAddress = "101 Oak Street", City = "Tulsa", State = "OK", ZipCode = "74955" } },
            //    new PersonModel { FirstName = "Trevor", LastName = "Fitzsimmons", DateOfBirth = new DateTime(1990, 12, 21, 0, 0, 0, DateTimeKind.Utc) }

            //};


            //Task.WaitAll(Task.Run(() => db.BulkInsertMongoDb("users", users)));

            //users.ForEach(u => { u.ToConsole(); });



            //db.Sort("users", "FirstName", users).ForEach(r => { r.ToJson().ToConsole(); });

            //TruncateCollection<PersonModel>("users");


            Console.WriteLine("Done.");
            Console.Read();
        }
        /// <summary>
        /// Runs a synchronous task for inserting into a collection using the table name and a record of a generic type.
        /// Outputs the record to the console.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="record"></param>
        public static void InsertRecord<T>(string table, T record)
        {
            try
            {
                db.InsertRecord(table, record);
                Console.WriteLine($"Inserted: " + record.ToJson());                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }
        /// <summary>
        /// Runs a synchronous task for a search all query on the table and returns the result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <returns></returns>
        public static List<T> LoadRecords<T>(string table)
        {
            List<T> records = null;
            try
            {
                records = db.LoadRecords<T>(table);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
            return records;
        }
        /// <summary>
        /// Runs a synchronous task for a search on the table using the unique id and returns the result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static T LoadRecordById<T>(string table, Guid id)
        {
            T record = default(T);
            try
            {
                record = db.LoadRecordById<T>(table, id);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
            return record;
        }
        /// <summary>
        /// List the databases for the server. 
        /// Outputs to the console.
        /// </summary>
        public static void ListDatabases()
        {
            try
            {
                Console.WriteLine($"The list of databases on this server are: ");
                db.GetDatabases().ToConsole();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }

        // TODO - DEPRICATE INTO THE MONGOACCESS LOGIC
        public static void Sort<T>(string table, string field, List<T> items)
        {
            try
            {
                var documents = db.Sort(table, field, items);
                documents.ToConsole();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }
        public void Project<T>(string table, string field, List<T> records)
        {
            try
            {
                var documents = db.Project(table, field, records);
                foreach (var doc in documents)
                {
                    Console.WriteLine(doc.ToJson());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }
        public static void Update<T>(string table, Guid id, T record)
        {
            try
            {
                db.UpsertRecord(table, id, record);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }
        public static void TruncateCollection<T>(string collectionName)
        {
            try
            {
                var result = db.TruncateCollection<T>(collectionName);
                Console.WriteLine($"Removed Records:  {result.IsAcknowledged} - Delete Count: {result.DeletedCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.GetBaseException());
            }
        }
        
    }

    class PersonModel
    {
        [BsonId]
        public Guid Id { get; set; }
        public string Email { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public AddressModel PrimaryAddress { get; set; }
        public DateTime DateOfBirth { get; set; }
        public void ToConsole()
        {
            Console.WriteLine($" { this.Id.ToString() }: { this.FirstName } { this.LastName } : { this.PrimaryAddress?.City }  {this.PrimaryAddress?.State } - { this.DateOfBirth } ");
        }
        
    }
    class AddressModel
    {
        public string StreetAddress { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string ZipCode { get; set; }
    }
    class MongoAccess
    {
        string _host;
        IMongoClient _client;
        IMongoDatabase _database;        
        public MongoAccess(string database, string host = "mongodb://localhost:27017")
        {
            _host = host;
            _client = new MongoClient(host);
            _database = _client.GetDatabase(database);            
        }
        /// <summary>
        /// Returns a collection using the collection name. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public IMongoCollection<T> GetCollection<T>(string collectionName)
        {
            try
            {
                return _database.GetCollection<T>(collectionName);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Retrieving Collection in Collection: { collectionName } " + ex.GetBaseException().ToString());
            }            
        }
        public void InsertRecord<T>(string collectionName, T record)
        {
            try
            {
                IMongoCollection<T> collection = this.GetCollection<T>(collectionName);
                collection.InsertOne(record);
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Inserting Record in Collection: { collectionName} " + ex.GetBaseException().ToString());
            }
        }
        /// <summary>
        /// Returns a list of generic documents using the collection name. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <returns></returns>
        public List<T> LoadRecords<T>(string collectionName)
        {
            try
            {
                IMongoCollection<T> collection = this.GetCollection<T>(collectionName);
                return collection.Find(new BsonDocument()).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Loading Records in Collection: { collectionName } " + ex.GetBaseException().ToString());
            }
        }
        /// <summary>
        /// Returnns a generic document using the collection name and the id parameters. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public T LoadRecordById<T>(string collectionName, Guid id)
        {   
            try
            {
                IMongoCollection<T> collection = this.GetCollection<T>(collectionName);
                FilterDefinition<T> filter = Builders<T>.Filter.Eq("Id", id);
                return collection.Find(filter).First();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Loading Record in Collection: { collectionName } " + ex.GetBaseException().ToString());
            }
           
        }
        /// <summary>
        /// Inserts the record if it doesn't exist. Updates the record if it does exist. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="id"></param>
        /// <param name="record"></param>
        public void UpsertRecord<T>(string collectionName, Guid id, T record)
        {
            IMongoCollection<T> collection;
            try
            {
                collection = this.GetCollection<T>(collectionName);
                var result = collection.ReplaceOne(new BsonDocument("_id", id), record, new ReplaceOptions { IsUpsert = true });
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Upserting Record in Collection: { collectionName} " + ex.GetBaseException().ToString());
            }
        }
        /// <summary>
        /// Inserts many records of a generic type into the collection using the collection name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="records"></param>
        /// <returns></returns>
        public async Task BulkInsertMongoDb<T>(string collectionName, List<T> records)
        {
            var listWrites = new List<WriteModel<T>>();
            records.ForEach(r => { listWrites.Add(new InsertOneModel<T>(r)); });
            IMongoCollection<T> collection = _database.GetCollection<T>(collectionName);
            var resultWrites = await collection.BulkWriteAsync(listWrites);

            Console.WriteLine($"OK?: {resultWrites.IsAcknowledged} - Inserted Count: {resultWrites.InsertedCount}");
        }
        /// <summary>
        /// Returns the databases in the mongo server.
        /// </summary>
        /// <returns></returns>
        public List<BsonDocument> GetDatabases()
        {
            try
            {
                return _client.ListDatabases().ToList();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Retrieving Databases from server: { _host }" + ex.GetBaseException().ToString());
            }
            
        }
        /// <summary>
        /// Delete one record matching the id in the collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="id"></param>
        public void DeleteRecord<T>(string collectionName, Guid id)
        {
            IMongoCollection<T> collection = this.GetCollection<T>(collectionName);
            var filter = Builders<T>.Filter.Eq("Id", id);
            collection.DeleteOne(filter);
        }
        
        /// <summary>
        /// Removes all records from the collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        public DeleteResult TruncateCollection<T>(string collectionName)
        {
            try
            {               
                var collection = _database.GetCollection<T>(collectionName);
                return collection.DeleteMany(new BsonDocument());                
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Truncating Collection: { collectionName } " + ex.GetBaseException().ToString());
            }
        }
        public List<T> Sort<T>(string table, string field, List<T> items)
        {
            try
            {
                // TODO: MONGOACCESS.SORT - UPDATE LOGIC TO SORT THE PARAMETER ITEMS INSTEAD OF THE COLLECTION 
                var collection = _database.GetCollection<T>(table);
                var filter = Builders<T>.Filter.Exists(field);
                var sort = Builders<T>.Sort.Descending(field);
                return collection.Find(filter).Sort(sort).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Sorting Records in table: { table } " + ex.GetBaseException().ToString());
            }
        }
        public List<BsonDocument> Project<T>(string collectionName, string field, List<T> records)
        {
            try
            {
                IMongoCollection<T> collection = _database.GetCollection<T>(collectionName);
                ProjectionDefinition<T> projection = Builders<T>.Projection.Exclude(field);
                return collection.Find(new BsonDocument()).Project(projection).ToList();
                
            }
            catch (Exception ex)
            {
                throw new Exception($"Error Projecting: in Collection: { collectionName } " + ex.GetBaseException().ToString());
            }
        }
    }
    static class ExtensionMethods
    {
        public static void ToConsole<T>(this T item)
        {
            Console.WriteLine($"{item}");
        }
        public static void ToConsole<T>(this List<T> items)
        {
            if(items.Count > 0)
            {
                items.ForEach(i => { Console.WriteLine($"{ i }"); });
            }
            Console.WriteLine($"No items for: " + items.ToString());
        }
    }
}

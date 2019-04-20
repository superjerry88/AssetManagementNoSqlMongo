using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace AssetManagementNoSqlMongo
{
    class Program
    {
        const string FilePath1 = @"C:\Users\Jerry\Downloads\7-8 mar.csv";
        const string FilePath2 = @"C:\Users\Jerry\Downloads\8-9 mar.csv";
        const string FilePath3 = @"C:\Users\Jerry\Downloads\9-10 mar.csv";
        const string FilePath4 = @"C:\Users\Jerry\Downloads\10-11 mar.csv";
        const string FilePath5 = @"C:\Users\Jerry\Downloads\11-12 mar.csv";

        static void Main(string[] args)
        {
            Console.WriteLine("Starting ...");
            Method1(FilePath5);
            Count1();
            CountCondition();
            FindMax();
            Console.ReadLine();
        }

        private static void Method1(string file)
        {
            var row = 172519;
            var stopwatch = Stopwatch.StartNew();
            AddData(GetCsvData2(file, row));
            Console.WriteLine($"Insert Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms \nTotal Data: {24 * row} \nAverage{stopwatch.ElapsedMilliseconds / (24.0 * row)}ms per record");
        }

        private static void Count1()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = CountHourlyData("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        private static void CountCondition()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = CountWithCondition("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"Count Condition, result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        private static void FindMax()
        {
            var stopwatch = Stopwatch.StartNew();
            var result = FindMax("Q1_Act ValueY");
            Console.WriteLine("---------------------------------------");
            Console.WriteLine($"FindMax, result: {result}");
            Console.WriteLine($"Code Complete. Time Taken: {stopwatch.ElapsedMilliseconds}ms ");
            GC.Collect();
        }

        public static void AddData(Dictionary<string, HourData> items)
        {
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<HourData>("HourData");

            foreach (var value in items.Values)
            {
                var exist = collection.FindSync(item => item.Id == value.Id).Any();
                if (exist)
                {
                    collection.ReplaceOne(i => i.Id == value.Id, value);
                }
                else
                {
                    collection.InsertOne(value);
                }
            }
        }


        public static void AddHourlyDataKeyToAsset(string assetid, string hourdataid)
        {
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<SensorAsset>("Assets");

            var exist = collection.FindSync(item => item.AssetId == assetid).Any();
            if (exist)
            {
                var asset = collection.FindSync(item => item.AssetId == assetid).First();
                asset.HourDatas.Add(hourdataid);
                collection.ReplaceOne(i => i.Id == assetid, asset);
            }
        }

        public static void CreateAssetIfNotExist(string asset)
        {
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<SensorAsset>("Assets");

            var exist = collection.FindSync(item => item.AssetId == asset).Any();
            if (!exist)
            {
                collection.InsertOneAsync(new SensorAsset
                {
                    Id = asset,
                    AssetId = asset
                });
            }    
        }


        public static int CountHourlyData(string assetId)
        {
            var count = 0;
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<HourData>("HourData").AsQueryable().Where(s => s.Asset == assetId);

            foreach (var data in collection)
            {
                count += data.Items.Count;
            }
            return count;
        }

        public static int CountWithCondition(string assetId)
        {
            var count = 0;
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<HourData>("HourData").AsQueryable().Where(s => s.Asset == assetId);

            foreach (var data in collection)
            {
                count += data.Items.Count(item => item.Value > 5);
            }
            return count;
        }

        public static double FindMax(string assetId)
        {
            double max = 0.00;
            var dbClient = new MongoClient("mongodb://127.0.0.1:27017");
            var database = dbClient.GetDatabase("TestDb1");
            var collection = database.GetCollection<HourData>("HourData").AsQueryable().Where(s => s.Asset == assetId);

            foreach (var data in collection)
            {
                var minimax = data.Items.Max(i => i.Value);
                if (minimax > max) max = minimax;
            }
            return max;
        }

        public static Dictionary<string, HourData> GetCsvData2(string filePath, int row)
        {
            using (var reader = new StreamReader(filePath))
            {
                var names = new List<string>();

                var asset = new Dictionary<string, HourData>();

                for (var i = 0; i < row && !reader.EndOfStream; i++)
                {
                    var line = reader.ReadLine();
                    var values = line.Split('\t');
                    if (i == 0)
                    {
                        names.AddRange(values);
                        foreach (var name in names.Where(c => !c.Contains("Time")))
                        {
                            CreateAssetIfNotExist(name.Replace("\"", ""));
                        }
                    }
                    else
                    {
                        for (var col = 0; col < values.Length; col++)
                        {
                            var assetName = names[col].Replace("\"", "");
                            if (assetName.Contains("Time")) continue;

                            var date = DateTime.Parse(values[0]);
                            var stringValue = values[col];
                            var item = double.TryParse(stringValue, out var number) ? new Item(number, date) : new Item(stringValue, date);
                            var key = $"SensorData_{assetName}_{date:yyyy_MM_dd_HH}";

                            if (!asset.ContainsKey(key))
                            {
                                asset[key] = new HourData(assetName, item.DateTime);
                                AddHourlyDataKeyToAsset(assetName, key);
                            }
                            asset[key].Items.Add(item);
                        }
                    }
                }
                return asset;
            }
        }
    }

    public class Asset
    {
        [BsonId]
        public string Id { get; set; }
        [BsonElement("AssetId")]
        public string AssetId { get; set; }
    }

    public class SensorAsset : Asset
    {
        [BsonElement("HourDatas")]
        public HashSet<string> HourDatas { get; set; } = new HashSet<string>();
    }

    public class HourData
    {
        [BsonId]
        public string Id { get; set; }
        [BsonElement("Asset")]
        public string Asset { get; set; }
        [BsonElement("DateTime")]
        public DateTime DateTime { get; set; }
        [BsonElement("Items")]
        public List<Item> Items { get; set; } = new List<Item>();

        public HourData(string asset, DateTime dt)
        {
            Asset = asset;
            Id = $"SensorData_{asset}_{dt:yyyy_MM_dd_HH}";
            DateTime = dt;

        }
    }

    public class Item
    {
        [BsonElement("Data")]
        public string Data { get; set; }
        [BsonElement("Value")]
        public double Value { get; set; }
        [BsonElement("DateTime")]
        public DateTime DateTime { get; set; }
        [BsonElement("IsValue")]
        public bool IsValue { get; set; }
        public string GetData() => IsValue ? Value.ToString() : Data;

        public Item(string data, DateTime? dt = null)
        {
            var datetime = DateTime.Now;
            if (dt != null) datetime = (DateTime)dt;
            Data = data;
            DateTime = datetime;
            IsValue = false;
        }

        public Item(double data, DateTime? dt = null)
        {
            var datetime = DateTime.Now;
            if (dt != null) datetime = (DateTime)dt;
            Value = data;
            DateTime = datetime;
            IsValue = true;
        }

        public Item()
        {
        }
    }
}

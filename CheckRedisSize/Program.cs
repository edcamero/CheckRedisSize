using System;
using ServiceStack.Redis;
using System.Text.RegularExpressions;
using System.Collections.Generic;
namespace CheckRedisSize
{
    class Program
    {
        public const float CONVERSION_CHECK = 1024f;
        public const string DELETE_COMMAND ="--delete";
        public static bool isErasable = false;
        public static string redisHost = "127.0.0.1";
        public static string port = "6379";
        public static double maxSize = 1;
        public static string mult = "bytes";


        static void Main(string[] args)
        {

            
            if (args.Length > 2)
            {
                DefineHost(args[0]);
                GetLimitOfArguments(args[1]);
                if(args.Length == 3 && args[2] == DELETE_COMMAND)
                {
                    isErasable = true;
                }
                CheckRedisSize();

            }
            else
            {
                Console.Error.Write("Invalid Arguments. ");
                Console.WriteLine("Plus Two necessary arguments:");
                Console.WriteLine("CheckRedisSize <RedisConnectionString> <maxSize>");
                Console.WriteLine("CheckRedisSize <localhost> <int><'byte'|'kb'|'mb'>");
            }
        }
        static void GetLimitOfArguments(string limitSize)
        {
            Match digits = Regex.Match(limitSize, "(\\d+)");
            Match measureUnit = Regex.Match(limitSize, "([a-z]+)");

            if (measureUnit.Success)
            {
                mult = measureUnit.Value;
            }
            if (digits.Success)
            {
                DefinMaxLimite(long.Parse(digits.Value));
            }

        }

        static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / CONVERSION_CHECK) / CONVERSION_CHECK;
        }
        static double ConvertBytesToKilobytes(long bytes)
        {
            return (bytes / CONVERSION_CHECK);
        }

        static double ConvertMegabytesToBytes(long megabytes)
        {
            return (megabytes * CONVERSION_CHECK) * CONVERSION_CHECK;
        }
        static double ConvertKilobytesToBytes(long kilobytes)
        {
            return (kilobytes * CONVERSION_CHECK);
        }

        static void DefinMaxLimite(long maxSizeArg)
        {
            switch (mult)
            {
                case "bytes":
                    maxSize = maxSizeArg;
                    break;
                case "kb":
                    maxSize = ConvertKilobytesToBytes(maxSizeArg);
                    break;
                case "mb":
                    maxSize = ConvertMegabytesToBytes(maxSizeArg);
                    break;
                default:
                    Console.WriteLine("Not an accepted unit");
                    break;


            }

        }




        static void CheckRedisSize()
        {
            using (var redisClient = new RedisClient(redisHost, Convert.ToInt16(port)))
            {

                double totalsize = 0;
                var keys = redisClient.GetAllKeys();
                Console.WriteLine(String.Format("|{0,10}|{1,15} |{2,22}|", "KEY", "SIZE", "TLL"));
                foreach (string key in keys)
                {
                    try
                    {
                        long size = redisClient.GetStringCount(key);
                        double kblen = ConvertBytesToKilobytes(size);
                        double mblen = ConvertBytesToMegabytes(size);

                        totalsize = totalsize + mblen;
                        PrintRow(key, size, redisClient.GetTimeToLive(key).Value.TotalSeconds);
                    }
                    catch (Exception ex)
                    {
                        try
                        {
                            byte[][] bythsharr = redisClient.HGetAll(key);
                            double kblen = ConvertBytesToKilobytes(bythsharr.Length);
                            double mblen = ConvertBytesToMegabytes(bythsharr.Length);
                            PrintRow(key, bythsharr.Length, redisClient.GetTimeToLive(key).Value.TotalSeconds);
                        }
                        catch (Exception ex1)
                        {
                            try
                            {
                                PrintRow(key, CalculateListSize(redisClient.GetAllItemsFromList(key)), redisClient.GetTimeToLive(key).Value.TotalSeconds);
                            }
                            catch (Exception ex2)
                            {
                                ;
                                PrintRow(key, CalculateHastSetySize(redisClient.GetAllItemsFromSet(key)), redisClient.GetTimeToLive(key).Value.TotalSeconds);

                            }
                        }
                    }
                }
            }

        }

        static long CalculateListSize(List<string> list)
        {
            long sizeAllList = 0;
            foreach (string item in list)
            {
                sizeAllList += item.Length;
            }
            return sizeAllList;
        }

        static long CalculateHastSetySize(HashSet<string> dictionary)
        {
            long sizeAllList = 0;
            foreach (string item in dictionary)
            {
                sizeAllList += item.Length;
            }
            return sizeAllList;
        }

        static void PrintRow(string key, long size, double seconds)
        {
            if (maxSize < size)
            {
                Console.WriteLine(String.Format("|{0,10}|{1,10} bytes|{2,10} Seg.|", key, size, seconds));
            }
        }
        static void DefineHost(String hostString)
        {
            int separator = 0;
            separator = hostString.LastIndexOf(':');
            if (separator == -1)
            {
                redisHost = hostString;
            }
            else
            {
                redisHost = hostString.Substring(0, separator);
                port = hostString.Substring(separator);
            }
        }

        static void DeleteKey(RedisClient redisClient, String key)
        {
            try
            {
                redisClient.Delete(key);
                Console.WriteLine(String.Format("Key {0} deleted", key));

            }
            catch(Exception exception)
            {
                Console.WriteLine(String.Format("Error: {0}", exception.Message));
            }
           
            
        }

    }
}

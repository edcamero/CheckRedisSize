using System;
using ServiceStack.Redis;
using System.Text.RegularExpressions;
namespace CheckRedisSize
{
    class Program
    {
        public static string redisHost = "127.0.0.1";
        public static string port = "6379";
        public static double maxSize = 1;
        public static string mult = "bytes";
        
        static void Main(string[] args)
        {
            
            int separator = 0; 
            Console.WriteLine();
            if (args.Length == 2)
            {   separator = args[0].LastIndexOf(':');
                if (separator == -1)
                {
                    redisHost = args[0];
                }
                else
                {
                    redisHost = args[0].Substring(0, separator);
                    port = args[0].Substring(separator);
                }
                getLimitOfArguments(args[1]);
                findlen();
            }           
            else
            {
                Console.Error.Write("Argumentos Invalidos. ");
                Console.WriteLine("Dos argumentos necesarios:");
                Console.WriteLine("<<RedisConnectionString>>");
                Console.WriteLine("<<SizeGreaterThan>>");
            }
        }
        static void getLimitOfArguments(string limiteSize)
        {
            Match digito = Regex.Match(limiteSize, "(\\d+)");
            Match units = Regex.Match(limiteSize, @"^[a-zA-Z]+$");

            string num = string.Empty;
            if (digito.Success)
            {
                maxSize = Int32.Parse(digito.Value);
            }
            if (units.Success)
            {
                mult = units.Value;
            }
        }

        static double ConvertBytesToMegabytes(long bytes)
        {
            return (bytes / 1024f) / 1024f;
        }
        static double ConvertBytesToKilobytes(long bytes)
        {
            return (bytes / 1024f);
        }

        static double ConvertMegabytesToBytes(long megabytes)
        {
            return (megabytes * 1024f) * 1024f;
        }
        static double ConvertKilobytesToBytes(long kilobytes)
        {
            return (kilobytes * 1024f);
        }

        static void DefineMaxLimite()
        {
            switch (mult){
                case "kb":
                    maxSize = ConvertBytesToKilobytes(maxSize);
                    break;
                
            }

        }


        static void findlen()
        {
            using (var redisClient = new RedisClient(redisHost, Convert.ToInt16(port)))
            {
                var config = new RedisEndpoint
                {
                    Host = redisHost,
                    Port = Int32.Parse(port),
                };

            
                double totalsize = 0;
                var keys = redisClient.GetAllKeys();
                Console.WriteLine("...Download...");
                Console.WriteLine("KEY"+"\t"+"SIZE"+ "\t\t\t" + "TLL");
                foreach (string key in keys)
                {
                    try
                    {
                        byte[] bytarr = redisClient.Get(key);
                        double kblen = ConvertBytesToKilobytes(bytarr.Length);
                        double mblen = ConvertBytesToMegabytes(bytarr.Length);
                        totalsize = totalsize + mblen;
                        Console.WriteLine(key + "\t" + mblen + "\t" + redisClient.GetTimeToLive(key).Value.TotalSeconds+ "Seg.");
                       

                    }
                    catch (Exception ex)
                    {
                        try
                        {
                            byte[][] bythsharr = redisClient.HGetAll(key);
                            double kblen = ConvertBytesToKilobytes(bythsharr.Length);
                            double mblen = ConvertBytesToMegabytes(bythsharr.Length);
                            Console.WriteLine("Hash Key Name : " + key + " Key length in MB : " + mblen + " Key Length in Kb : " + kblen);
                           
                        }
                        catch (Exception ex1)
                        {

                        }
                    }
                }
            }
        }

    }

}

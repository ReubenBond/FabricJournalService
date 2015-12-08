using System;
using System.Threading.Tasks;

namespace TestApp
{
    using System.Diagnostics;
    using System.Fabric;

    using DistributedJournalService.Interfaces;

    using Microsoft.ServiceFabric.Services.Client;
    using Microsoft.ServiceFabric.Services.Communication.Client;
    using Microsoft.ServiceFabric.Services.Communication.Wcf.Client;

    internal class Program
    {
        private static void Main(string[] args)
        {
            Run(args).Wait();

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static async Task Run(string[] args)
        {
            // The Fabric URI of the service.
            var serviceName = new Uri("fabric:/DistributedJournalApp/DistributedJournalService");
            var serviceResolver = new ServicePartitionResolver(() => new FabricClient());

            var clientFactory = new WcfCommunicationClientFactory<IKeyValueStore>(
                serviceResolver,
                ServiceBindings.TcpBinding);

            var client = new ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>>(
                clientFactory,
                serviceName,
                partitionKey: 0L);

            Console.WriteLine("Calling set");
            await Set(client, "test", 38);
            Console.WriteLine("Set complete, calling get");
            Console.WriteLine($"Got {await Get(client, "test")}");

            var numTasks = args.Length == 0 ? 1 : int.Parse(args[0]);
            var tasks = new Task[numTasks];
            var iteration = (long)0;

            // Initialize.
            for (var i = 0; i < tasks.Length; ++i)
            {
                await Set(client, i.ToString(), iteration);
                //Console.WriteLine($"Completed Set {i} =  {iteration}");
            }

            var timer = Stopwatch.StartNew();
            while (true)
            {
                for (var i = 0; i < tasks.Length; ++i)
                {
                    tasks[i] = CheckAndIncrement(client, i.ToString(), iteration);
                    //Console.WriteLine($"Completed CheckAndIncrement {i} iteration {iteration}");
                }

                await Task.WhenAll(tasks);

                var total = iteration * tasks.Length;
                if (iteration % 100 == 0)
                {
                    //Console.Write('.');
                    Console.WriteLine($"{iteration} iterations in {timer.ElapsedMilliseconds}ms. {total * 1000 / (timer.ElapsedMilliseconds)}/sec");
                }

                if (iteration % 8000 == 0 && timer.ElapsedMilliseconds > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine($"{iteration} iterations in {timer.ElapsedMilliseconds}ms. {total * 1000 / (timer.ElapsedMilliseconds)}/sec");
                }

                iteration++;

                if (Console.KeyAvailable)
                {
                    var consoleKey = Console.ReadKey();
                    if (consoleKey.Key == ConsoleKey.Escape)
                    {
                        break;
                    }
                }
            }
        }

        private static async Task CheckAndIncrement(ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client, string key, long expected)
        {
            var serialized = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key));
            var value = BitConverter.ToInt64(serialized, 0);

            if (value != expected)
            {
                throw new Exception($"[{DateTime.Now}] Expected {expected} but encountered {value}.");
            }

            await client.InvokeWithRetryAsync(_ => _.Channel.Set(key, BitConverter.GetBytes(value + 1)));
        }

        private static async Task<long> Get(ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client, string key)
        {
            var value = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key));
            return BitConverter.ToInt64(value, 0);
        }

        private static Task Set(ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client, string key, long value)
        {
            return client.InvokeWithRetryAsync(_ => _.Channel.Set(key, BitConverter.GetBytes(value)));
        }
    }
}

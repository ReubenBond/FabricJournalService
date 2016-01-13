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
        private const string DebugDumpDirectory = @"c:\tmp\fabbers\";

        private static void Main(string[] args)
        {
            Console.WriteLine(
                "Usage:\n" + "\t[Esc]\tQuit\n" + "\t[s]\tPrint status\n" + "\t[d]\tSend 'dump debug data' command\n"
                + "\t[p]\tPause execution\n");
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
            }

            var timer = Stopwatch.StartNew();
            var paused = false;
            while (true)
            {
                var total = iteration * tasks.Length;
                if (!paused)
                {
                    for (var i = 0; i < tasks.Length; ++i)
                    {
                        tasks[i] = CheckAndIncrement(client, i.ToString(), iteration);
                    }

                    await Task.WhenAll(tasks);

                    if (iteration % 8000 == 0 && iteration > 0)
                    {
                        Console.WriteLine();
                    }

                    if (iteration % 100 == 0)
                    {
                        WriteProgress(iteration, timer, total);
                    }
                    
                    iteration++;
                }

                // Process user input.
                if (Console.KeyAvailable || paused)
                {
                    var consoleKey = Console.ReadKey();
                    switch (consoleKey.Key)
                    {
                        case ConsoleKey.Escape:
                            return;
                        case ConsoleKey.D:
                            // Output debug data.
                            var prefix = $"{iteration}_";
                            await client.InvokeWithRetry(_ => _.Channel.DumpDebugData(DebugDumpDirectory, prefix));
                            break;
                        case ConsoleKey.P:
                            // Toggle pause
                            paused = !paused;
                            break;
                        case ConsoleKey.S:
                            WriteProgress(iteration, timer, total);
                            break;

                    }
                }
            }
        }

        private static void WriteProgress(long iteration, Stopwatch timer, long total)
        {
            Console.WriteLine(
                $"{iteration} iterations in {timer.ElapsedMilliseconds}ms. {total * 1000 / (timer.ElapsedMilliseconds)}/sec");
        }

        private static async Task CheckAndIncrement(
            ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client,
            string key,
            long expected)
        {
            var serialized = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key));
            var value = BitConverter.ToInt64(serialized, 0);

            if (value != expected)
            {
                throw new Exception($"[{DateTime.Now}] Expected {expected} but encountered {value}.");
            }

            await client.InvokeWithRetryAsync(_ => _.Channel.Set(key, BitConverter.GetBytes(value + 1)));
        }

        private static async Task<long> Get(
            ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client,
            string key)
        {
            var value = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key));
            return BitConverter.ToInt64(value, 0);
        }

        private static Task Set(
            ServicePartitionClient<WcfCommunicationClient<IKeyValueStore>> client,
            string key,
            long value)
        {
            return client.InvokeWithRetryAsync(_ => _.Channel.Set(key, BitConverter.GetBytes(value)));
        }
    }
}
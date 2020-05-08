namespace BlobClientModule
{
    using System;
    using System.IO;
    using System.Diagnostics;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Azure;
    using Azure.Storage.Blobs;

    class Program
    {
        static BlobContainerClient ContainerClient;
        static int Interval = 3 * 60 * 1000; // 3 min

        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
            Console.WriteLine($"{connectionString}");
            ContainerClient = new BlobContainerClient(connectionString, "samplecontainer");
            try
            {
                await ContainerClient.CreateIfNotExistsAsync();
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"{ex}");
            }

            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

#pragma warning disable 4014
            MainLoop(ioTHubModuleClient);
#pragma warning restore 4014
        }

        static async Task UploadBlobAsync(ModuleClient moduleClient)
        {
            try
            {
                string filePath = $"telemetry_data_{DateTime.UtcNow.ToString("yyyyMMdd_HHmmss")}.json";
                string data = "{ \"message\": \"Hello World\" }";
                BlobClient blobClient = ContainerClient.GetBlobClient(filePath);
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(data)))
                {
                    var result = await blobClient.UploadAsync(stream);
                    Console.WriteLine($"{result.ToString()}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex}");
            }
        }

        static async Task MainLoop(ModuleClient moduleClient)
        {
            Console.WriteLine("Start MainLoop");
            Stopwatch sw = new Stopwatch();
            while (true)
            {
                await UploadBlobAsync(moduleClient);
                await Task.Delay(Interval);
            }
        }
    }
}

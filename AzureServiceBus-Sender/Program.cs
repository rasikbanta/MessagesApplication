using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.KeyVault;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace AzureServiceBus_Sender
{
    class Program
    {
        //const string serviceBusConnStringFromVault;
        const string APP_CLIENT_ID = "ed0cb3f8-5b13-4e17-be3f-e621c8e290e8";
        const string APP_CLIENT_SECRET = "7E=Tjzf67iE-ZttTbi3JJsh@hbOTTfE_";
        const string KEY_VAULT_URI = "https://rbkeyvault-sb.vault.azure.net/";
        const string QueueName = "test-ras-queue";
        static IQueueClient queueClient;
        static IKeyVaultClient keyVaultClient;

        static async Task Main(string[] args)
        {
            //get the key from keyVault
            //keyVaultClient = new KeyVaultClient()

            string connectionStringFromVault = await GetConnectionStringSecret();
            while (true)
            {
                Console.WriteLine("press 1 to send messages, 2 to receive messages, 3 to exit");
                switch (Console.ReadLine())
                {
                    case "1":
                        Console.WriteLine("You chose to send messages. Write Message and press enter");
                        string strMessage = Console.ReadLine();
                        MainAsyncSendMessages(strMessage, connectionStringFromVault).GetAwaiter().GetResult();
                        break;
                    case "2":
                        Console.WriteLine("You chose to receive messages");
                        MainAsyncReceiveMesssages(connectionStringFromVault).GetAwaiter().GetResult();
                        break;
                    case "3":
                        Console.WriteLine("You chose to exit");
                        return;
                    default:
                        Console.WriteLine("Invalid option");
                        break;
                }
            }
            
        }

        /// <summary>
        /// Gets the connection string value from KeyVault
        /// </summary>
        /// <returns></returns>
        static async Task<string> GetConnectionStringSecret()
        {
            var kvc = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                async (string authority, string resource, string scope) => {
                    var authContext = new AuthenticationContext(authority);
                    var credential = new ClientCredential(APP_CLIENT_ID, APP_CLIENT_SECRET);

                    AuthenticationResult result = await authContext.AcquireTokenAsync(resource, credential);

                    if (result == null)
                    {
                        throw new InvalidOperationException("Failed to retrieve secret");
                    }
                    return result.AccessToken;
                }
            ));


            var secretBundle = await kvc.GetSecretAsync(KEY_VAULT_URI, "ServiceBusConnString1");

            return secretBundle.Value;
        }

        /// <summary>
        /// Send Messages to Queue
        /// </summary>
        /// <returns></returns>
        static async Task MainAsyncSendMessages(string message, string ServiceBusConnectionString)
        {
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            // Send messages.
            await SendMessagesAsync(message);

            Console.ReadKey();

            await queueClient.CloseAsync();
        }

        /// <summary>
        /// Receive Messages from Queue
        /// </summary>
        /// <returns></returns>
        static async Task MainAsyncReceiveMesssages(string ServiceBusConnectionString)
        {
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            // Register the queue message handler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();

            await queueClient.CloseAsync();
        }

        private static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 3,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false,
            };

            // Register the function that processes messages.
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task SendMessagesAsync(string message)
        {
            try
            {               
                // Create a new message to send to the queue.
                var sbMessage = new Message(Encoding.UTF8.GetBytes(message));

                // Write the body of the message to the console.
                Console.WriteLine($"Sending message: {message}");

                // Send the message to the queue.
                await queueClient.SendAsync(sbMessage);
                await queueClient.CloseAsync();
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }


        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
            //Console.WriteLine($"Message is locked until: {message.SystemProperties.LockedUntilUtc}. Time now is: {DateTime.UtcNow}");
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);
            await queueClient.CloseAsync();

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.
        }


        // Use this handler to examine the exceptions received on the message pump.
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}

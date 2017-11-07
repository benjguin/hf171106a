using System;
using Microsoft.Extensions.Configuration;


namespace GetAccesToStorageThruKeyVault
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // service principal created per https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal
                string appId = "##obfuscated##"; //object ID
                string appSecret = "##obfuscated##"; // app registration key
                string clientId = "##obfuscated##"; // Application ID
                string tenantId = "##obfuscated##"; // Directory ID

                // give access to the app to the key vault 

                string keyname = "test";

                var builder = new ConfigurationBuilder();
                builder.AddAzureKeyVault("https://##obfuscated##.vault.azure.net/", clientId, appSecret);

                var config = builder.Build();
                Console.WriteLine(config[keyname]);
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception: {ex}");
            }
            Console.WriteLine(".");
            Console.ReadLine();
        }
    }
}

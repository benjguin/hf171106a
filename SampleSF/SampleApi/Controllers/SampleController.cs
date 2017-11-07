using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;

namespace SampleApi.Controllers
{
    [Route("api/[controller]")]
    public class SampleController : Controller
    {
        private const string AuthorityUri = "https://login.microsoftonline.com/";
        private const string BatchResourceUri = "https://batch.core.windows.net/";

        // this method could be moved to another class like a security helper
        public static async Task<string> GetAuthenticationTokenAsync()
        {
            // values that will be in config afterwards
            string tenantId = "##obfuscated##";
            string appId = "##obfuscated##";
            string thumbprint = "##obfuscated##";

            string authority = AuthorityUri + tenantId;

            X509Certificate2 cert;
            X509Certificate2Collection certCollection;
            using (X509Store certStore = new X509Store(StoreName.My, StoreLocation.LocalMachine))
            {
                // Try to open the store.

                certStore.Open(OpenFlags.ReadOnly);
                // Find the certificate that matches the thumbprint.
                certCollection = certStore.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);
            }

            // Check to see if our certificate was added to the collection. If no, throw an error, if yes, create a certificate using it.
            if (0 == certCollection.Count)
            {
                throw new ApplicationException($"Error: No certificate found with thumbprint '{thumbprint}'");
            }
            cert = certCollection[0];
            var clientCredential = new ClientAssertionCertificate(appId, cert);
            var authenticationContext = new AuthenticationContext(authority, false);
            var result = await authenticationContext.AcquireTokenAsync(BatchResourceUri, clientCredential);

            return result.AccessToken;
        }

        // GET api/Sample
        [HttpGet]
        public string Get()
        {
            return "sample API";
        }

        // GET api/Sample/test-access
        [Route("test-access")]
        [HttpGet]
        public async Task<string> TestAccess()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Start Test");

            Func<Task<string>> tokenProvider = () => GetAuthenticationTokenAsync();

            try
            {
                // values that will be in config afterwards
                string batchAccountUrl = "https://obfuscated.westeurope.batch.azure.com";

                using (var client = await BatchClient.OpenAsync(new BatchTokenCredentials(batchAccountUrl, tokenProvider)))
                {
                    // add a retry policy. The built-in policies are No Retry (default), Linear Retry, and Exponential Retry
                    client.CustomBehaviors.Add(RetryPolicyProvider.NoRetryProvider());

                    var stats = await client.JobOperations.GetAllLifetimeStatisticsAsync();
                    sb.AppendLine($"got statistics. SucceededTaskCount = {stats.SucceededTaskCount}");
                }
            }
            catch (Exception ex)
            {
                sb.AppendLine($"Exception: {ex}");
            }

            return sb.ToString();
        }
    }
}

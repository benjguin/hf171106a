using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.Azure.Batch.Conventions.Files;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace xgridbatch
{
    class Program
    {

        private const string AuthorityUri = "https://login.microsoftonline.com/";
        private const string BatchResourceUri = "https://batch.core.windows.net/";
        private const string BatchAccountUrl = "https://###yourbatchaccount###.yourregion.batch.azure.com";

        private const bool ShouldDeleteJob = false;

        // AppId or ClientId
        private const string ClientId = "<clientid>";
        // AppKey, AppSecret or ClientKey
        private const string ClientKey = "<clientkey>";
        private const string TenantId = "<tenantid>";

        // SAS key for app storage
        private const string appStoreSASKey = "<storage account sas key with container and object RW access>";

        static void Main(string[] args)
        {

            try
            {
                MainAsync().Wait();
            }
            catch (AggregateException ae)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();

                foreach (Exception exception in ae.InnerExceptions)
                {
                    Console.WriteLine(exception.ToString());
                    Console.WriteLine();
                }
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        private static async Task MainAsync()
        {
            await PerformBatchOperations();
        }

        public static async Task<string> GetAuthenticationTokenAsync()
        {
            AuthenticationContext authContext = new AuthenticationContext(AuthorityUri + TenantId);
            AuthenticationResult authResult = await authContext.AcquireTokenAsync(BatchResourceUri, new ClientCredential(ClientId, ClientKey));

            return authResult.AccessToken;
        }

        public static async Task PerformBatchOperations()
        {
            Func<Task<string>> tokenProvider = () => GetAuthenticationTokenAsync();
            //            StorageCredentials storageCred = new StorageCredentials(appStoreSASKey);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(appStoreSASKey);

            using (var client = await BatchClient.OpenAsync(new BatchTokenCredentials(BatchAccountUrl, tokenProvider)))
            {
                // add a retry policy. The built-in policies are No Retry (default), Linear Retry, and Exponential Retry
                client.CustomBehaviors.Add(RetryPolicyProvider.ExponentialRetryProvider(TimeSpan.FromSeconds(15), 3));

                string jobId = "xgrid";
                string poolId = "xgrid-pool";

                try
                {
                    // Create a CloudPool, or obtain an existing pool with the specified ID
                    var poolSettings = new PoolSettings
                    {
                        PoolTargetNodeCount = 2,
                        PoolNodeVirtualMachineSize = "Standard_D2_V2",
                        MaxTasksPerNode = 2,
                        subnetId = "<your subnet resource id>"
                    };

                    CloudPool pool = await CreateVMPoolIfNotExistAsync(client, poolId, poolSettings);

                    // Create a CloudJob, or obtain an existing pool with the specified ID
                    CloudJob job = await CreateJobIfNotExistAsync(client, poolId, jobId);

                    // Get the container URL to use
                    string containerName = job.OutputStorageContainerName();
                    CloudBlobContainer container = storageAccount.CreateCloudBlobClient().GetContainerReference(containerName);
                    await container.CreateIfNotExistsAsync();

                    // build the container SAS URL, job.GetOutputStorageContainerUrl(storageAccount) create a SAS key but for this 
                    // you need the account key whihc is not avaialbe here

                    string containerUrl = $"{storageAccount.BlobEndpoint}{containerName}?{storageAccount.Credentials.SASToken}";


                    // create a simple task. Each task within a job must have a unique ID
                    var task = new CloudTask(Guid.NewGuid().ToString(), "cmd /c echo Hello world from the Batch Hello world sample!");

                    task.WithOutputFile(@"..\std*.txt", containerUrl, TaskOutputKind.TaskLog, OutputFileUploadCondition.TaskCompletion);

                    await client.JobOperations.AddTaskAsync(jobId, task);

                    // Wait for the job to complete
                    await WaitForJobAndPrintOutputAsync(client, jobId);
                }
                finally
                {
                    // Delete the job to ensure the tasks are cleaned up
                    if (!string.IsNullOrEmpty(jobId) && ShouldDeleteJob)
                    {
                        Console.WriteLine("Deleting job: {0}", jobId);
                        await client.JobOperations.DeleteJobAsync(jobId);
                    }
                }

            }
        }

        /// <summary>
        /// Creates a <see cref="CloudPool"/> associated with the specified Batch account. If an existing pool with the
        /// specified ID is found, the pool is resized to match the specified node count.
        /// </summary>
        /// <param name="batchClient">A fully initialized <see cref="BatchClient"/>.</param>
        /// <param name="poolId">The ID of the <see cref="CloudPool"/>.</param>
        /// <param name="nodeSize">The size of the nodes within the pool.</param>
        /// <param name="nodeCount">The number of nodes to create within the pool.</param>
        /// <param name="maxTasksPerNode">The maximum number of tasks to run concurrently on each node.</param>
        /// <returns>A bound <see cref="CloudPool"/> with the specified properties.</returns>
        public async static Task<CloudPool> CreateVMPoolIfNotExistAsync(BatchClient batchClient, string poolId, PoolSettings poolSettings)
        {
            // look at documentation from here https://docs.microsoft.com/en-us/azure/batch/batch-linux-nodes

            // Obtain a collection of all available node agent SKUs.
            // This allows us to select from a list of supported
            // VM image/node agent combinations.
            List<NodeAgentSku> nodeAgentSkus = batchClient.PoolOperations.ListNodeAgentSkus().ToList();

            // Define a delegate specifying properties of the VM image
            // that we wish to use.
            Func<ImageReference, bool> isWinServer2016 = imageRef =>
                imageRef.Publisher == "MicrosoftWindowsServer" &&
                imageRef.Offer == "WindowsServer" &&
                imageRef.Sku.Contains("2016-Datacenter");

            // Obtain the first node agent SKU in the collection that matches
            // Windows Server 2016. Note that there are one or more image
            // references associated with this node agent SKU.
            NodeAgentSku winServerAgentSku = nodeAgentSkus.First(sku =>
                sku.VerifiedImageReferences.Any(isWinServer2016));

            // Select an ImageReference from those available for node agent.
            ImageReference imageReference = winServerAgentSku.VerifiedImageReferences.First(isWinServer2016);

            // Create the VirtualMachineConfiguration for use when actually
            // creating the pool
            VirtualMachineConfiguration virtualMachineConfiguration = new VirtualMachineConfiguration(imageReference, winServerAgentSku.Id);

            // Create the unbound pool object using the VirtualMachineConfiguration
            // created above
            CloudPool pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                virtualMachineSize: poolSettings.PoolNodeVirtualMachineSize,
                virtualMachineConfiguration: virtualMachineConfiguration,
                targetDedicatedComputeNodes: poolSettings.PoolTargetNodeCount);

            pool.MaxTasksPerComputeNode = poolSettings.MaxTasksPerNode;
            pool.NetworkConfiguration = new NetworkConfiguration();
            pool.NetworkConfiguration.SubnetId = poolSettings.subnetId;

            // We want each node to be completely filled with tasks (i.e. up to maxTasksPerNode) before
            // tasks are assigned to the next node in the pool
            pool.TaskSchedulingPolicy = new TaskSchedulingPolicy(ComputeNodeFillType.Pack);

            await BatchCommon.CreatePoolIfNotExistAsync(batchClient, pool).ConfigureAwait(continueOnCapturedContext: false);

            return await batchClient.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);
        }

        /// <summary>
        /// Creates a CloudJob in the specified pool if a job with the specified ID is not found
        /// in the pool, otherwise returns the existing job.
        /// </summary>
        /// <param name="batchClient">A fully initialized <see cref="BatchClient"/>.</param>
        /// <param name="poolId">The ID of the CloudPool in which the job should be created.</param>
        /// <param name="jobId">The ID of the CloudJob.</param>
        /// <returns>A bound version of the newly created CloudJob.</returns>
        public static async Task<CloudJob> CreateJobIfNotExistAsync(BatchClient batchClient, string poolId, string jobId)
        {
            CloudJob job = await BatchCommon.GetJobIfExistAsync(batchClient, jobId).ConfigureAwait(continueOnCapturedContext: false);

            if (job == null)
            {
                Console.WriteLine("Job {0} not found, creating...", jobId);

                CloudJob unboundJob = batchClient.JobOperations.CreateJob(jobId, new PoolInformation() { PoolId = poolId });
                await unboundJob.CommitAsync().ConfigureAwait(continueOnCapturedContext: false);

                // Get the bound version of the job with all of its properties populated
                job = await batchClient.JobOperations.GetJobAsync(jobId).ConfigureAwait(continueOnCapturedContext: false);
            }

            return job;
        }

        /// <summary>
        /// Creates a job and adds a task to it.
        /// </summary>
        /// <param name="batchClient">The BatchClient to use when interacting with the Batch service.</param>
        /// <param name="configurationSettings">The configuration settings</param>
        /// <param name="jobId">The ID of the job.</param>
        /// <returns>An asynchronous <see cref="Task"/> representing the operation.</returns>
        private static async Task SubmitJobAsync(BatchClient batchClient, PoolSettings poolSettings, string jobId)
        {
            // create an empty unbound Job
            CloudJob unboundJob = batchClient.JobOperations.CreateJob();
            unboundJob.Id = jobId;

            // For this job, ask the Batch service to automatically create a pool of VMs when the job is submitted.
            unboundJob.PoolInformation = new PoolInformation()
            {
                AutoPoolSpecification = new AutoPoolSpecification()
                {
                    AutoPoolIdPrefix = "HelloWorld",
                    PoolSpecification = new PoolSpecification()
                    {
                        TargetDedicatedComputeNodes = poolSettings.PoolTargetNodeCount,
                        CloudServiceConfiguration = new CloudServiceConfiguration(poolSettings.PoolOSFamily),
                        VirtualMachineSize = poolSettings.PoolNodeVirtualMachineSize,
                    },
                    KeepAlive = false,
                    PoolLifetimeOption = PoolLifetimeOption.Job
                }
            };

            // Commit Job to create it in the service
            await unboundJob.CommitAsync();

            // create a simple task. Each task within a job must have a unique ID
            await batchClient.JobOperations.AddTaskAsync(jobId, new CloudTask("task1", "cmd /c echo Hello world from the Batch Hello world sample!"));
        }

        /// <summary>
        /// Waits for all tasks under the specified job to complete and then prints each task's output to the console.
        /// </summary>
        /// <param name="batchClient">The BatchClient to use when interacting with the Batch service.</param>
        /// <param name="jobId">The ID of the job.</param>
        /// <returns>An asynchronous <see cref="Task"/> representing the operation.</returns>
        private static async Task WaitForJobAndPrintOutputAsync(BatchClient batchClient, string jobId)
        {
            Console.WriteLine("Waiting for all tasks to complete on job: {0} ...", jobId);

            // We use the task state monitor to monitor the state of our tasks -- in this case we will wait for them all to complete.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

            List<CloudTask> ourTasks = await batchClient.JobOperations.ListTasks(jobId).ToListAsync();

            // Wait for all tasks to reach the completed state.
            // If the pool is being resized then enough time is needed for the nodes to reach the idle state in order
            // for tasks to run on them.
            await taskStateMonitor.WhenAll(ourTasks, TaskState.Completed, TimeSpan.FromMinutes(10));

            // dump task output
            foreach (CloudTask t in ourTasks)
            {
                Console.WriteLine("Task {0}", t.Id);

                //Read the standard out of the task
                NodeFile standardOutFile = await t.GetNodeFileAsync(Constants.StandardOutFileName);
                string standardOutText = await standardOutFile.ReadAsStringAsync();
                Console.WriteLine("Standard out:");
                Console.WriteLine(standardOutText);

                Console.WriteLine();
            }
        }

        public static string CreateJobId(string prefix)
        {
            // a job is uniquely identified by its ID so your account name along with a timestamp is added as suffix
            return string.Format("{0}-{1}-{2}", prefix, new string(Environment.UserName.Where(char.IsLetterOrDigit).ToArray()), DateTime.Now.ToString("yyyyMMdd-HHmmss"));
        }
    }
}

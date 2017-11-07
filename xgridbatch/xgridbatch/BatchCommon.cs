using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace xgridbatch
{
    public class BatchCommon
    {
        public enum CreatePoolResult
        {
            PoolExisted,
            CreatedNew,
            ResizedExisting,
        }

        /// <summary>
        /// Creates a pool if it doesn't already exist.  If the pool already exists, this method resizes it to meet the expected
        /// targets specified in settings.
        /// </summary>
        /// <param name="batchClient">The BatchClient to create the pool with.</param>
        /// <param name="pool">The pool to create.</param>
        /// <returns>An asynchronous <see cref="Task"/> representing the operation.</returns>
        public static async Task<CreatePoolResult> CreatePoolIfNotExistAsync(BatchClient batchClient, CloudPool pool)
        {
            bool successfullyCreatedPool = false;

            int targetDedicatedNodeCount = pool.TargetDedicatedComputeNodes ?? 0;
            int targetLowPriorityNodeCount = pool.TargetLowPriorityComputeNodes ?? 0;
            string poolNodeVirtualMachineSize = pool.VirtualMachineSize;
            string poolId = pool.Id;

            // Attempt to create the pool
            try
            {
                // Create an in-memory representation of the Batch pool which we would like to create.  We are free to modify/update 
                // this pool object in memory until we commit it to the service via the CommitAsync method.
                Console.WriteLine("Attempting to create pool: {0}", pool.Id);

                // Create the pool on the Batch Service
                await pool.CommitAsync().ConfigureAwait(continueOnCapturedContext: false);

                successfullyCreatedPool = true;
                Console.WriteLine("Created pool {0} with {1} dedicated and {2} low priority {3} nodes",
                    poolId,
                    targetDedicatedNodeCount,
                    targetLowPriorityNodeCount,
                    poolNodeVirtualMachineSize);
            }
            catch (BatchException e)
            {
                // Swallow the specific error code PoolExists since that is expected if the pool already exists
                if (e.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    // The pool already existed when we tried to create it
                    successfullyCreatedPool = false;
                    Console.WriteLine("The pool already existed when we tried to create it");
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }

            // If the pool already existed, make sure that its targets are correct
            if (!successfullyCreatedPool)
            {
                CloudPool existingPool = await batchClient.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);

                // If the pool doesn't have the right number of nodes, isn't resizing, and doesn't have
                // automatic scaling enabled, then we need to ask it to resize
                if ((existingPool.CurrentDedicatedComputeNodes != targetDedicatedNodeCount || existingPool.CurrentLowPriorityComputeNodes != targetLowPriorityNodeCount) &&
                    existingPool.AllocationState != AllocationState.Resizing &&
                    existingPool.AutoScaleEnabled == false)
                {
                    // Resize the pool to the desired target. Note that provisioning the nodes in the pool may take some time
                    await existingPool.ResizeAsync(targetDedicatedNodeCount, targetLowPriorityNodeCount).ConfigureAwait(continueOnCapturedContext: false);
                    return CreatePoolResult.ResizedExisting;
                }
                else
                {
                    return CreatePoolResult.PoolExisted;
                }
            }

            return CreatePoolResult.CreatedNew;
        }

        /// <summary>
        /// Returns an existing <see cref="CloudJob"/> if found in the Batch account.
        /// </summary>
        /// <param name="batchClient">A fully initialized <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The <see cref="CloudJob.Id"/> of the desired pool.</param>
        /// <returns>A bound <see cref="CloudJob"/>, or <c>null</c> if the specified <see cref="CloudJob"/> does not exist.</returns>
        public static async Task<CloudJob> GetJobIfExistAsync(BatchClient batchClient, string jobId)
        {
            Console.WriteLine("Checking for existing job {0}...", jobId);

            // Construct a detail level with a filter clause that specifies the job ID so that only
            // a single CloudJob is returned by the Batch service (if that job exists)
            ODATADetailLevel detail = new ODATADetailLevel(filterClause: string.Format("id eq '{0}'", jobId));
            List<CloudJob> jobs = await batchClient.JobOperations.ListJobs(detailLevel: detail).ToListAsync().ConfigureAwait(continueOnCapturedContext: false);

            return jobs.FirstOrDefault();
        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace xgridbatch
{
    public class PoolSettings
    {
        public int PoolTargetNodeCount { get; set; }
        public string PoolOSFamily { get; set; }
        public string PoolNodeVirtualMachineSize { get; set; }
        public int MaxTasksPerNode { get; set; }
        public string subnetId { get; set; }
    }
}

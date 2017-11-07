using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace SampleApi.Controllers
{
    [Route("api/[controller]")]
    public class SampleController : Controller
    {
        // GET api/Sample
        [HttpGet]
        public string Get()
        {
            return "test";
        }
    }
}

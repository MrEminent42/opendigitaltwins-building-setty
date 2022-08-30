using System;
using Neo4jClient;
using Neo4jClient.Cypher;
using Neo4j.Driver;
using System.Threading.Tasks;
using Microsoft.Azure.DigitalTwins.Parser;
using System.Collections.Generic;
using System.Text.Json;
using System.Text;
using System.IO;
using Helper;
using System.Linq;

namespace AzureDTDL
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // string WILLOW_PATH = @"C:\Users\micro\dev\setty\ontology\Ontology\Willow";
            // string OPENTWINS_PATH = @"C:\Users\micro\dev\setty\ontology\Ontology\opendigitaltwins-building\Ontology";
            string WILLOW_PATH = @"..\Ontology\Willow";
            string OPENTWINS_PATH = @"..\Ontology\opendigitaltwins-building\Ontology";

            List<string> files = new List<string>();
            files.AddRange(Directory.GetFiles(WILLOW_PATH, "*", SearchOption.AllDirectories));
            files.AddRange(Directory.GetFiles(OPENTWINS_PATH, "*", SearchOption.AllDirectories));

            OntologyUploader uploader = new OntologyUploader();


            await uploader.ProcessFiles(files);


        }



    }
}

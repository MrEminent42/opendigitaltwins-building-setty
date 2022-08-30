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

    class OntologyUploader
    {
        IDriver _driver;
        BoltGraphClient client;


        public OntologyUploader()
        {
            _driver = GraphDatabase.Driver(
                            "bolt://localhost:7687/db/data",
                            AuthTokens.Basic("neo4j", "syyclops"),
                            c => c.WithEncryptionLevel(EncryptionLevel.None)
                        );
            client = new BoltGraphClient(_driver);
        }

        public async Task ProcessFiles(List<string> filePaths)
        {
            await client.ConnectAsync();

            try
            {
                // Load all the model text.
                var modelTexts = new Dictionary<string, string>();

                // load all files
                foreach (string path in filePaths)
                {
                    modelTexts.Add(path, System.IO.File.ReadAllText(path));
                }

                // Filter out invalid models
                Dictionary<string, string> validModelTexts = FindValidJsonFiles(modelTexts);
                if (validModelTexts.Count != modelTexts.Count)
                {
                    // throw new Exception("Could not parse JSON files.");
                    Console.Error.WriteLine("WARNING! Could not parse all JSON files.");
                }

                // Parse models
                IReadOnlyDictionary<Dtmi, DTEntityInfo> entities = await ParseModelsAsync(validModelTexts);
                if (entities == null)
                {
                    throw new Exception("Entities were null. Error parsing entities.");
                }

                List<DTEntityInfo> secondRound = (await UploadFirstRound(entities.Values.ToList())).ToList();
                // await UploadSecondRound(secondRound);
                // now uploading immediately as part of first round


            }
            catch (Exception e)
            {
                client.Dispose();
                Console.WriteLine("\n\n-----------");
                Console.WriteLine("\n\nERROR: " + e);
                Console.WriteLine("\n\n-----------");

                throw e;
            }

            client.Dispose();


        }



        private async Task<HashSet<DTEntityInfo>> UploadFirstRound(List<DTEntityInfo> entities)
        {
            IEnumerable<IGrouping<DTEntityKind, DTEntityInfo>> groups = entities.GroupBy(
                    (entity) => entity.EntityKind);


            foreach (IGrouping<DTEntityKind, DTEntityInfo> group in groups)
            {
                DTEntityKind entityKind = group.Key;
                List<DTEntityInfo> groupEntities = group.ToList();
                Console.WriteLine("Processing EntityKind " + entityKind);

                // save relationships for later
                if (entityKind == DTEntityKind.Relationship)
                {
                    // TODO: deal with relationships
                    // secondRound.UnionWith(groupEntities.Cast<DTRelationshipInfo>().ToList());
                    continue;
                }

                if (entityKind != DTEntityKind.Interface)
                {
                    Console.WriteLine("\tNo upload implementation for EntityKind " + entityKind);
                    continue;
                }

                int BATCH_SIZE = 1;
                // create batches
                for (int batch = 260; batch < (groupEntities.Count / BATCH_SIZE) + 1; batch++)
                {
                    Console.Write($"Batch {batch} ");

                    HashSet<DTEntityInfo> secondRound = new HashSet<DTEntityInfo>();
                    switch (entityKind)
                    {
                        case DTEntityKind.Interface:
                            secondRound.UnionWith(await UploadInterfaces(groupEntities.GetRange(batch * BATCH_SIZE, BATCH_SIZE).Cast<DTInterfaceInfo>().ToList()));
                            break;
                        // case DTEntityKind.Telemetry:
                        //     break;
                        // case DTEntityKind.Property:
                        //     break;
                        // case DTEntityKind.Command:
                        //     break;
                        // case DTEntityKind.Component:
                        //     break;
                        default:
                            Console.WriteLine("\tNo upload implementation for EntityKind " + entityKind);
                            break;
                    }

                    await UploadSecondRound(secondRound.ToList());
                }
            }

            // return secondRound;
            return new HashSet<DTEntityInfo>();

        }

        // type-specific upload functions

        public async Task<HashSet<DTEntityInfo>> UploadInterfaces(List<DTInterfaceInfo> interfaces)
        {
            var thisCommand = client
                        .Cypher;

            // save stuff to deal with in the second round
            HashSet<DTEntityInfo> secondRound = new HashSet<DTEntityInfo>();

            Console.Write("\tInterfaces: ");

            // loop through all interfaces
            for (int i = 0; i < interfaces.Count(); i++)
            {
                try
                {
                    // interface var setup
                    DTInterfaceInfo interfaceEntity = interfaces[i];
                    string interfaceIdentifier = "interface" + i;
                    string interfaceName = StringHelper.SpacesToCamel(
                        StringHelper.ReplaceNonAlphanumeric(StringHelper.GetNameFromJsonId(interfaceEntity.Id.ToString()), ' ')
                        );
                    // upload/find this interface node
                    thisCommand = thisCommand.Merge($"({interfaceIdentifier}:{Labels.Interface} " + "{" + Props.Name + ": '" + interfaceName + "'})");

                    // props for this interface node
                    PropsBuilder interfaceProps = new PropsBuilder(interfaceIdentifier);
                    Console.Write(interfaceName + ", ");

                    // upload name, Id, comment, description
                    interfaceProps.Add(Props.Name, interfaceName);
                    if (interfaceEntity.Id != null)
                    {
                        interfaceProps.Add(Props.JsonId, interfaceEntity.Id.AbsoluteUri);
                    }
                    if (interfaceEntity.Comment != null)
                    {
                        interfaceProps.Add(Props.Comment, interfaceEntity.Comment);
                    }
                    if (interfaceEntity.Description != null && interfaceEntity.Description.ContainsKey("en"))
                    {
                        interfaceProps.Add(Props.Description, interfaceEntity.Description["en"]);
                    }
                    thisCommand = interfaceProps.ApplyProps(thisCommand);


                    // add parent/:extends relationships
                    for (int j = 0; j < interfaceEntity.Extends.Count(); j++)
                    {
                        DTInterfaceInfo parent = interfaceEntity.Extends[j];
                        string parentName = StringHelper.SpacesToCamel(
                            StringHelper.ReplaceNonAlphanumeric(
                            StringHelper.GetNameFromJsonId(parent.Id.ToString()), ' ')
                            );
                        string parentIdentifier = $"{interfaceIdentifier}{j}parent";
                        if (parentName.Equals(interfaceName)) continue; // prevent extending itself - bug from Willow's assets extending DTDL's original assets



                        // create/find parent interface node
                        thisCommand = thisCommand.Merge($"({parentIdentifier}:{Labels.Interface} " + "{" + Props.Name + ": '" + parentName + "'})");
                        thisCommand = thisCommand.Merge($"({parentIdentifier})<-[:"
                                + Relationships.Extends + "]-(" + interfaceIdentifier + ")");
                    }

                    // bare bones upload: complex schemas will only have their name and jsonId uploaded
                    // plan for connecting schemas to nodes TBD
                    for (int j = 0; j < interfaceEntity.Schemas.Count(); j++)
                    {
                        string schemaIdentifier = $"{interfaceIdentifier}{j}schema";
                        DTComplexSchemaInfo schema = interfaceEntity.Schemas[j];
                        secondRound.Add(schema);
                        thisCommand = thisCommand
                                    .Merge($"({schemaIdentifier}:{Labels.ComplexSchema}:{Labels.Schema}"
                                        + "{" + Props.JsonId + ": '" + schema.Id.ToString() + "'})");

                        PropsBuilder schemaProps = new PropsBuilder(schemaIdentifier);
                        schemaProps.Add(Props.Name, StringHelper.GetNameFromJsonId(schema.Id.ToString()));
                        thisCommand = schemaProps.ApplyProps(thisCommand);
                    }

                    // bare bones upload: content will only have their name, jsonId, and schemas uploaded
                    // the rest will come in the second round
                    // also.. only dealing with properties right now, not other types of contents
                    int contentIndex = 0;
                    foreach ((string _, DTContentInfo content) in interfaceEntity.Contents)
                    {
                        string contentLabel = "";
                        string contentIdentifier = $"{interfaceIdentifier}{contentIndex}content";
                        switch (content.EntityKind)
                        {
                            case DTEntityKind.Property:
                                contentLabel = Labels.Property;
                                break;
                            default:
                                // TODO: other content entity kinds
                                continue;
                        }
                        thisCommand = thisCommand.Merge($"({contentIdentifier}:{contentLabel}:{Labels.Content} "
                                + "{" + Props.JsonId + ": '" + content.Id + "'})"); // TODO put schema on here
                        thisCommand = thisCommand.Merge($"({interfaceIdentifier})-[:{Relationships.HasContent}]-({contentIdentifier})");

                        // upload name, Id, comment, description
                        PropsBuilder contentProps = new PropsBuilder(contentIdentifier);
                        contentProps.Add(Props.Name, content.Name);
                        if (content.Id != null) contentProps.Add(Props.JsonId, content.Id.AbsoluteUri);
                        if (content.Comment != null) contentProps.Add(Props.Comment, content.Comment);
                        if (content.Description != null && content.Description.ContainsKey("en")) contentProps.Add(Props.Description, content.Description["en"]);
                        thisCommand = contentProps.ApplyProps(thisCommand);


                        // add schema
                        var contentSchema = GetSchema(content);
                        if (contentSchema != null)
                        {
                            string contentSchemaIdentifier = $"{contentIdentifier}schema";
                            thisCommand = thisCommand.Merge(
                                $"({contentSchemaIdentifier}:{Labels.Schema}:{(contentSchema is DTComplexSchemaInfo ? Labels.ComplexSchema : Labels.PrimitieSchema)} "
                                + "{" + Props.JsonId + ": '" + contentSchema.Id.ToString() + "'})");
                            thisCommand = thisCommand.Merge($"({contentSchemaIdentifier})<-[:{Relationships.HasSchema}]-({contentIdentifier})");

                            PropsBuilder contentSchemaProps = new PropsBuilder(contentSchemaIdentifier);
                            contentSchemaProps.Add(Props.Name, StringHelper.GetNameFromJsonId(contentSchema.Id.ToString()));
                            thisCommand = contentSchemaProps.ApplyProps(thisCommand);
                        }

                        secondRound.Add(contentSchema);
                        contentIndex++;
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    Console.WriteLine("tried to upload but couldn't deal with " + interfaces[i].Id.AbsoluteUri);
                }
            }

            Console.Write("\n\tUploading...");
            await thisCommand.ExecuteWithoutResultsAsync();
            Console.WriteLine(" Done!");

            return secondRound;
        }

        public async Task UploadSecondRound(List<DTEntityInfo> entities)
        {
            // second round
            // currently: interface schemas and interface content schemas (property schemas)
            Console.Write("\tProcessing second round...");

            var todoEntities = new Queue<DTEntityInfo>(entities);

            int index = 0;
            while (todoEntities.Count() != 0)
            {
                var thisCommand = client
                        .Cypher;
                DTEntityInfo entity = todoEntities.Dequeue();
                string entityIdentifier = "entity" + index;
                if (entity is DTPrimitiveSchemaInfo)
                {
                    // nothing to be done here... all primitives
                    DTPrimitiveSchemaInfo primitiveSchemaInfo = (DTPrimitiveSchemaInfo)entity;
                    continue;
                }
                // Console.Write("\tUploading... ");
                if (entity is DTArrayInfo)
                {
                    // Console.WriteLine("Uploading ARRAY!");
                    DTArrayInfo arrayInfo = (DTArrayInfo)entity;
                    string arrayIdentifier = entityIdentifier;
                    string elementSchemaIdentifier = arrayIdentifier + "schema";
                    thisCommand = thisCommand
                        .Merge($"({arrayIdentifier}:{Labels.Schema}" + "{" + Props.JsonId + ": '" + arrayInfo.Id.ToString() + "'})")
                        .Merge($"({elementSchemaIdentifier}:{Labels.Schema} " + "{" + Props.JsonId + ": '" + arrayInfo.ElementSchema.Id + "'})")
                        .Merge($"({elementSchemaIdentifier})<-[:{Relationships.HasElementSchema}]-({arrayIdentifier})")
                        .Set($"{arrayIdentifier}:{Labels.Array}");


                    PropsBuilder arrayProps = new PropsBuilder(arrayIdentifier);
                    if (arrayInfo.Comment != null) arrayProps.Add(Props.Comment, arrayInfo.Comment);
                    if (arrayInfo.Description.ContainsKey("en")) arrayProps.Add(Props.Description, arrayInfo.Description["en"]);
                    if (arrayInfo.DisplayName.ContainsKey("en")) arrayProps.Add(Props.Name, arrayInfo.DisplayName["en"]);
                    thisCommand = arrayProps.ApplyProps(thisCommand);

                    todoEntities.Enqueue(arrayInfo.ElementSchema);

                }
                else if (entity is DTEnumInfo)
                {
                    // Console.WriteLine("Uploading ENUM!");
                    DTEnumInfo enumInfo = (DTEnumInfo)entity;
                    string enumIdentifier = entityIdentifier;
                    string enumValueSchemaIdentifier = enumIdentifier + "valueSchema";
                    thisCommand = thisCommand
                        .Merge($"({enumIdentifier}:{Labels.Schema} " + "{" + Props.JsonId + ": '" + enumInfo.Id.ToString() + "'})")
                        .Merge($"({enumValueSchemaIdentifier}:{Labels.Schema} " + "{" + Props.JsonId + ": '" + enumInfo.ValueSchema.Id.ToString() + "'})")
                        .Merge($"({enumIdentifier})-[:{Relationships.HasValueSchema}]->({enumValueSchemaIdentifier})")
                        .Set($"{enumIdentifier}:{Labels.Enum}");

                    PropsBuilder enumProps = new PropsBuilder(enumIdentifier);
                    if (enumInfo.Comment != null) enumProps.Add(Props.Comment, enumInfo.Comment);
                    if (enumInfo.Description.ContainsKey("en")) enumProps.Add(Props.Description, enumInfo.Description["en"]);
                    if (enumInfo.DisplayName.ContainsKey("en")) enumProps.Add(Props.Name, enumInfo.DisplayName["en"]);
                    thisCommand = enumProps.ApplyProps(thisCommand);

                    for (int j = 0; j < enumInfo.EnumValues.Count(); j++)
                    {
                        DTEnumValueInfo enumValue = enumInfo.EnumValues[j];
                        string enumValueIdentifier = enumIdentifier + "option" + j;
                        PropsBuilder enumValueProps = new PropsBuilder(enumValueIdentifier);
                        if (enumValue.Name != null) enumValueProps.Add(Props.Name, enumValue.Name);
                        if (enumValue.Comment != null) enumValueProps.Add(Props.Comment, enumValue.Comment);
                        if (enumValue.Description.ContainsKey("en")) enumValueProps.Add(Props.Description, enumValue.Description["en"]);
                        thisCommand = thisCommand
                            .Merge($"({enumValueIdentifier}:{Labels.EnumValue} " + "{" + Props.JsonId + ": '" + enumValue.Id.ToString() + "'})")
                            .Merge($"({enumValueIdentifier})<-[:{Relationships.HasValue}]-({enumIdentifier})");
                        thisCommand = enumValueProps.ApplyProps(thisCommand);
                    }

                }
                else if (entity is DTMapInfo)
                {
                    // Console.WriteLine("Uploading MAP!");
                    DTMapInfo mapInfo = (DTMapInfo)entity;
                    DTMapKeyInfo key = mapInfo.MapKey;
                    DTMapValueInfo val = mapInfo.MapValue;
                    string mapIdentifier = entityIdentifier;
                    string keyIdentifier = mapIdentifier + "key";
                    string valIdentifier = mapIdentifier + "val";
                    string keySchemaIdentifier = keyIdentifier + "schema";
                    string valSchemaIdentifier = valIdentifier + "schema";

                    // add map node
                    thisCommand = thisCommand
                        .Merge($"({mapIdentifier}:{Labels.Schema} " + "{" + Props.JsonId + ": '" + mapInfo.Id.ToString() + "'})")
                        .Set($"{mapIdentifier}:{Labels.Map}");
                    // add map props
                    PropsBuilder mapProps = new PropsBuilder(mapIdentifier);
                    if (mapInfo.Comment != null) mapProps.Add(Props.Comment, mapInfo.Comment);
                    if (mapInfo.Description.ContainsKey("en")) mapProps.Add(Props.Description, mapInfo.Description["en"]);
                    if (mapInfo.DisplayName.ContainsKey("en")) mapProps.Add(Props.Name, mapInfo.DisplayName["en"]);
                    thisCommand = mapProps.ApplyProps(thisCommand);

                    // add key node & connect it to map
                    thisCommand = thisCommand
                        .Merge($"({keyIdentifier}:{Labels.MapKey} " + "{" + Props.JsonId + ": '" + key.Id.ToString() + "'})")
                        .Merge($"({keyIdentifier})<-[:{Relationships.HasKey}]-({mapIdentifier})");
                    // add key node props
                    PropsBuilder keyProps = new PropsBuilder(keyIdentifier);
                    keyProps.Add(Props.Name, key.Name);
                    if (key.Comment != null) keyProps.Add(Props.Comment, key.Comment);
                    if (key.Description.ContainsKey("en")) keyProps.Add(Props.Description, key.Description["en"]);
                    thisCommand = keyProps.ApplyProps(thisCommand);
                    // add key schema 
                    thisCommand = thisCommand.Merge($"({keySchemaIdentifier}:{Labels.Schema} "
                        + "{" + Props.JsonId + ": '" + key.Schema.Id.ToString() + "'})");
                    thisCommand = thisCommand.Merge($"({keySchemaIdentifier})<-[:{Relationships.HasSchema}]-({keyIdentifier})");

                    // add value & connect it to map
                    thisCommand = thisCommand
                        .Merge($"({valIdentifier}:{Labels.MapValue} " + "{" + Props.JsonId + ": '" + val.Id.ToString() + "'})")
                        .Merge($"({valIdentifier})<-[:{Relationships.HasValue}]-({mapIdentifier})");
                    // add value props
                    PropsBuilder valProps = new PropsBuilder(valIdentifier);
                    valProps.Add(Props.Name, val.Name);
                    if (val.Comment != null) valProps.Add(Props.Comment, val.Comment);
                    if (val.Description.ContainsKey("en")) valProps.Add(Props.Description, val.Description["en"]);
                    thisCommand = valProps.ApplyProps(thisCommand);
                    // add value schema
                    thisCommand = thisCommand.Merge($"({valSchemaIdentifier}:{Labels.Schema} "
                        + "{" + Props.JsonId + ": '" + val.Schema.Id.ToString() + "'})");
                    thisCommand = thisCommand.Merge($"({valSchemaIdentifier})<-[:{Relationships.HasSchema}]-({valIdentifier})");


                    todoEntities.Enqueue(key.Schema);
                    todoEntities.Enqueue(val.Schema);

                }
                else if (entity is DTObjectInfo)
                {
                    DTObjectInfo objectInfo = (DTObjectInfo)entity;
                    string objectIdentifier = entityIdentifier;

                    // add object node
                    thisCommand = thisCommand
                        .Merge($"({objectIdentifier}:{Labels.Schema} " + "{" + Props.JsonId + ": '" + objectInfo.Id.ToString() + "'})")
                        .Set($"{objectIdentifier}:{Labels.Object}");
                    // add object props
                    PropsBuilder objectProps = new PropsBuilder(objectIdentifier);
                    if (objectInfo.Comment != null) objectProps.Add(Props.Comment, objectInfo.Comment);
                    if (objectInfo.Description.ContainsKey("en")) objectProps.Add(Props.Description, objectInfo.Description["en"]);
                    if (objectInfo.DisplayName.ContainsKey("en")) objectProps.Add(Props.Name, objectInfo.DisplayName["en"]);

                    for (int j = 0; j < objectInfo.Fields.Count(); j++)
                    {
                        DTFieldInfo fieldInfo = objectInfo.Fields[j];
                        string fieldIdentifier = objectIdentifier + "field" + j;
                        string fieldSchemaIdentifier = fieldIdentifier + "schema";

                        // create field node
                        thisCommand = thisCommand
                            .Merge($"({fieldIdentifier}:{Labels.Field} " + "{" + Props.JsonId + ": '" + fieldInfo.Id.ToString() + "'})")
                            .Merge($"({fieldIdentifier})<-[:{Relationships.HasField}]-({objectIdentifier})");
                        // field props
                        PropsBuilder fieldProps = new PropsBuilder(fieldIdentifier);
                        if (fieldInfo.Name != null) fieldProps.Add(Props.Name, fieldInfo.Name);
                        if (fieldInfo.Comment != null) fieldProps.Add(Props.Comment, fieldInfo.Comment);
                        if (fieldInfo.Description.ContainsKey("en")) fieldProps.Add(Props.Description, fieldInfo.Description["en"]);
                        thisCommand = fieldProps.ApplyProps(thisCommand);
                        // field schema
                        thisCommand = thisCommand.Merge($"({fieldSchemaIdentifier}:{Labels.Schema} "
                            + "{" + Props.JsonId + ": '" + fieldInfo.Schema.Id.ToString() + "'})");
                        thisCommand = thisCommand.Merge($"({fieldSchemaIdentifier})<-[:{Relationships.HasSchema}]-({fieldIdentifier})");


                        todoEntities.Enqueue(fieldInfo.Schema);
                    }

                }
                // else if (entity is DTFieldInfo) {

                // }
                else
                {
                    Console.WriteLine("Unknown second round item kind " + entity.EntityKind);
                    continue;
                }

                index++;

                // Console.Write("Uploading... ");
                await thisCommand.ExecuteWithoutResultsAsync();
            }
            Console.WriteLine("Done!");


        }




        // helper methods

        private static async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> ParseModelsAsync(Dictionary<string, string> modelTexts)
        {
            IReadOnlyDictionary<Dtmi, DTEntityInfo> entities = null;
            try
            {
                var parser = new ModelParser();
                entities = await parser.ParseAsync(modelTexts.Values);
            }
            catch (ParsingException ex)
            {
                Console.Error.WriteLine("Errors parsing models.");
                foreach (ParsingError error in ex.Errors)
                {
                    Console.Error.WriteLine(error.Message);
                }
                throw ex;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Errors parsing models.");
                Console.WriteLine(ex.Message);

                throw ex;
            }

            return entities;
        }

        private static Dictionary<string, string> FindValidJsonFiles(Dictionary<string, string> modelTexts)
        {
            var jsonErrors = new Dictionary<string, System.Text.Json.JsonException>();
            var valid = new Dictionary<string, string>();
            foreach (string fileName in modelTexts.Keys)
            {
                try
                {
                    JsonDocument jsonDoc = JsonDocument.Parse(modelTexts[fileName]);
                    valid.Add(fileName, modelTexts[fileName]);
                }
                catch (System.Text.Json.JsonException ex)
                {
                    jsonErrors.Add(fileName, ex);
                }
            }

            if (jsonErrors.Count > 0)
            {
                Console.Error.WriteLine("Errors parsing models:");
                foreach (string fileName in jsonErrors.Keys)
                {
                    Console.Error.WriteLine($"{fileName}: {jsonErrors[fileName].Message}");
                }
            }

            return valid;
        }

        public static void PrintInterfaceContent(DTInterfaceInfo dtif, IReadOnlyDictionary<Dtmi, DTEntityInfo> dtdlOM, int indent = 0)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < indent; i++) sb.Append("  ");
            Console.WriteLine($"{sb}Interface: {dtif.Id} | {dtif.DisplayName}");
            Dictionary<string, DTContentInfo> contents = dtif.Contents;
        }

        public static DTSchemaInfo GetSchema(DTContentInfo content)
        {
            if (content is DTTelemetryInfo) return ((DTTelemetryInfo)content).Schema;
            if (content is DTPropertyInfo) return ((DTPropertyInfo)content).Schema;
            // if (content is DTCommandInfo) return ((DTCommandInfo) content).Schema;
            // if (content is DTRelationshipInfo) return ((DTRelationshipInfo) content).Schema;
            // if (content is DTComponentInfo) return ((DTComponentInfo) content).Schema;
            return null;
        }


    }
}
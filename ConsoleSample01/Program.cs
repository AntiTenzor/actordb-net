using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

using ActorDb;

namespace ConsoleSample01
{
    internal class Program
    {
        static void Main(string[] args)
        {
            using (var client = new ActorDbClient("localhost", 33306, null))
            {
                {
                    //var t = client.LoginSecureAsync("myuser", "mypass");
                    var t = client.LoginSecureAsync("root", "rootpass");
                    t.Wait();
                    bool ok = t.Result;
                    if (!ok)
                        throw new InvalidOperationException("Is there some error?");
                }

                {
                    var t = client.GetProtocolVersionAsync();
                    t.Wait();
                    string protocolVer = t.Result;
                    Console.WriteLine("Protocol ver   : {0}", protocolVer);
                }

                // Throws exception here. 
                // Should I make some additional configurations?
                //{
                //    var t = client.GetLocalNodeNameAsync();
                //    t.Wait();
                //    string localNodeName = t.Result;
                //    Console.WriteLine("local node name: {0}", localNodeName);
                //}

                Configuration conf;
                {
                    var t = client.GetConfigurationAsync();
                    t.Wait();
                    conf = t.Result;
                    Console.WriteLine("There are {0} groups:", conf.Groups.Count);
                    foreach (Group group in conf.Groups)
                        Console.WriteLine($"  Name:{group.Name}; type:{group.Type}");
                    Console.WriteLine();

                    Console.WriteLine("There are {0} nodes:", conf.Nodes.Count);
                    foreach (Node nod in conf.Nodes)
                        Console.WriteLine($"  Name:{nod.Name} in group {nod.Group.Name}");
                    Console.WriteLine();
                }

                IReadOnlyCollection<string> actorTypes;
                {
                    var t = client.GetActorTypesAsync();
                    t.Wait();
                    actorTypes = t.Result;
                    Console.WriteLine("There are {0} actor types:", actorTypes.Count);
                    foreach (string at in actorTypes)
                        Console.WriteLine("  {0}", at);
                    Console.WriteLine();
                }

                var allActorTablesByTypes = new Dictionary<string, IReadOnlyCollection<string>>();
                {
                    foreach (string at in actorTypes)
                    {
                        var t = client.GetActorTablesAsync(at);
                        t.Wait();
                        IReadOnlyCollection<string> actorTables = t.Result;
                        allActorTablesByTypes[at] = actorTables;

                        Console.WriteLine("Actor type '{0}' defines {1} tables:", at, actorTables.Count);
                        foreach (string table in actorTables)
                            Console.WriteLine("  table: {0}", table);
                        Console.WriteLine();
                    }

                    Console.WriteLine();
                }

                {
                    var t = client.ExecSingleAsync("music", "type1", "SELECT * FROM tab", new List<string>());
                    t.Wait();
                    var tuple = t.Result;
                    // If everything is OK, then there is no exception in Item2
                    if ((tuple.Item1 != null) && (tuple.Item2 == null))
                    {
                        Console.WriteLine("    i   |   txt");
                        var listOfRows = tuple.Item1;
                        for (int j = 0; j < listOfRows.Count; j++)
                        {
                            Dictionary<string, Val> row = listOfRows[j];

                            string stop = "";

                            if (row.TryGetValue("i", out Val ival) &&
                                row.TryGetValue("txt", out Val tval))
                            {
                                Console.WriteLine($"  {ival} | {tval}");
                            }
                        }
                        Console.WriteLine();
                    }
                    Console.WriteLine();
                }

                {
                    var t = client.ExecAllAsync("type1", "{{RESULT}} SELECT * FROM tab", new List<string>());
                    t.Wait();
                    var tuple = t.Result;
                    // If everything is OK, then there is no exception in Item2
                    if ((tuple.Item1 != null) && (tuple.Item2 == null))
                    {
                        Console.WriteLine("    actor   |   i   |   txt");
                        var listOfRows = tuple.Item1;
                        for (int j = 0; j < listOfRows.Count; j++)
                        {
                            Dictionary<string, Val> row = listOfRows[j];

                            string stop = "";
                            if (row.TryGetValue("actor", out Val aval) &&
                                row.TryGetValue("i", out Val ival) &&
                                row.TryGetValue("txt", out Val tval))
                            {
                                Console.WriteLine($"  {aval}    | {ival} | {tval}");
                            }
                        }
                        Console.WriteLine();
                    }
                    Console.WriteLine();
                }

                {
                    var t = client.ExecSqlAsync("   SELECT 1 AS i, 'Just a test' AS txt   ");
                    t.Wait();
                    var tuple = t.Result;
                    // If everything is OK, then there is no exception in Item2
                    if ((tuple.Item1 != null) && (tuple.Item2 == null))
                    {
                        Console.WriteLine("    i   |   txt");
                        var listOfRows = tuple.Item1;
                        for (int j = 0; j < listOfRows.Count; j++)
                        {
                            Dictionary<string, Val> row = listOfRows[j];

                            string stop = "";

                            if (row.TryGetValue("i", out Val ival) &&
                                row.TryGetValue("txt", out Val tval))
                            {
                                Console.WriteLine($"  {ival} | {tval}");
                            }
                        }
                        Console.WriteLine();
                    }
                    else
                    {
                        Console.WriteLine("   SOME ERROR!");
                        if (tuple.Item2 != null)
                            Console.WriteLine(tuple.Item2);
                    }
                    Console.WriteLine();
                }

                {
                    // Flag 'CREATE' makes server to create new actor automatically
                    string actorName = Guid.NewGuid().ToString("N");
                    actorName = "rnd" + actorName.ToUpperInvariant().Substring(0, 1);

                    // INSERT statement modifies wrRes and reports number of modified rows
                    var t = client.ExecSingleAsync(
                        actorName, "type1",
                        "   INSERT INTO tab (i,txt) VALUES (17,\"Test INSERT with random actor\")   ",
                        new List<string>() { "CREATE" });
                    t.Wait();
                    var tuple = t.Result;
                    // If everything is OK, then there is no exception in Item2
                    if ((tuple.Item1 != null) && (tuple.Item2 == null))
                    {
                        Console.WriteLine("    i   |   txt");
                        var listOfRows = tuple.Item1;
                        for (int j = 0; j < listOfRows.Count; j++)
                        {
                            Dictionary<string, Val> row = listOfRows[j];

                            string stop = "";

                            if (row.TryGetValue("i", out Val ival) &&
                                row.TryGetValue("txt", out Val tval))
                            {
                                Console.WriteLine($"  {ival} | {tval}");
                            }
                        }
                        Console.WriteLine();
                    }
                    else
                    {
                        Console.WriteLine("   SOME ERROR!");
                        if (tuple.Item2 != null)
                            Console.WriteLine(tuple.Item2);
                    }
                    Console.WriteLine();
                }
            }
        }
    }
}

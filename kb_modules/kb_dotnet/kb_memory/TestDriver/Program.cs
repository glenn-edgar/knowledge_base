using System;
using KbMemoryModule;

namespace TestDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Testing KB Memory Module");
            
            // Test BasicConstructDb
            var basicDb = new BasicConstructDb();
            BasicConstructDb.InitializeDatabase();
            
            // Test ConstructMemDB
            var memDb = new ConstructMemDB();
            memDb.BuildMemoryDatabase();
            var table = memDb.GetMemoryTable();
            
            // Test SearchMemDB
            var searchDb = new SearchMemDB(table);
            
            Console.WriteLine("KB Memory Module tests completed");
        }
    }
}

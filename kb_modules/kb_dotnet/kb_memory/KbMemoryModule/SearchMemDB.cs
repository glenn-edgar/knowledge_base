using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace KbMemoryModule
{
    public class SearchMemDB
    {
        private readonly DataTable _dataTable;
        
        public SearchMemDB(DataTable dataTable)
        {
            _dataTable = dataTable;
        }
        
        public DataRow[] SearchByColumn(string columnName, object value)
        {
            return _dataTable.Select($"{columnName} = '{value}'");
        }
        
        public List<DataRow> SearchByMultipleColumns(Dictionary<string, object> searchCriteria)
        {
            // Add your multi-column search logic here
            return new List<DataRow>();
        }
    }
}

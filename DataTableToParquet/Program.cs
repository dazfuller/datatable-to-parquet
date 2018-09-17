using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Parquet;
using Parquet.Data;
using DataColumn = System.Data.DataColumn;
using DBNull = System.DBNull;

namespace DataTableToParquet
{
    public class Program
    {
        // The size of a row group, this is pretty low but is simply for demonstration
        private const int RowGroupSize = 100;

        private const string OutputFilePath = "example.parquet";

        public static void Main(string[] args)
        {
            var dt = GenerateTestData();
            var fields = GenerateSchema(dt);

            // Open the output file for writing
            using (var stream = File.Open(OutputFilePath, FileMode.Create, FileAccess.Write))
            {
                using (var writer = new ParquetWriter(new Schema(fields), stream))
                {
                    var startRow = 0;

                    // Keep on creating row groups until we run out of data
                    while (startRow < dt.Rows.Count)
                    {
                        using (var rgw = writer.CreateRowGroup(RowGroupSize))
                        {
                            // Data is written to the row group column by column
                            for (var i = 0; i < dt.Columns.Count; i++)
                            {
                                var columnIndex = i;

                                // Determine the target data type for the column
                                var targetType = dt.Columns[columnIndex].DataType;
                                if (targetType == typeof(DateTime)) targetType = typeof(DateTimeOffset);

                                // Generate the value type, this is to ensure it can handle null values
                                var valueType = targetType.IsClass
                                    ? targetType
                                    : typeof(Nullable<>).MakeGenericType(targetType);

                                // Create a list to hold values of the required type for the column
                                var list = (IList)typeof(List<>)
                                    .MakeGenericType(valueType)
                                    .GetConstructor(Type.EmptyTypes)
                                    .Invoke(null);

                                // Get the data to be written to the parquet stream
                                foreach (var row in dt.AsEnumerable().Skip(startRow).Take(RowGroupSize))
                                {
                                    // Check if value is null, if so then add a null value
                                    if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
                                    {
                                        list.Add(null);
                                    }
                                    else
                                    {
                                        // Add the value to the list, but if it's a DateTime then create it as a DateTimeOffset first
                                        list.Add(dt.Columns[columnIndex].DataType == typeof(DateTime)
                                            ? new DateTimeOffset((DateTime) row[columnIndex])
                                            : row[columnIndex]);
                                    }
                                }

                                // Copy the list values to an array of the same type as the WriteColumn method expects
                                // and Array
                                var valuesArray = Array.CreateInstance(valueType, list.Count);
                                list.CopyTo(valuesArray, 0);

                                // Write the column
                                rgw.WriteColumn(new Parquet.Data.DataColumn(fields[i], valuesArray));
                            }
                        }

                        startRow += RowGroupSize;
                    }
                }
            }
        }

        /// <summary>
        /// Creates a collection of Parquet fields from the <see cref="System.Data.DataTable"/>
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        private static List<DataField> GenerateSchema(DataTable dt)
        {
            var fields = new List<DataField>(dt.Columns.Count);

            foreach (DataColumn column in dt.Columns)
            {
                // Attempt to parse the type of column to a parquet data type
                var success = Enum.TryParse<DataType>(column.DataType.Name, true, out var type);

                // If the parse was not successful and it's source is a DateTime then use a DateTimeOffset, otherwise default to a string
                if (!success && column.DataType == typeof(DateTime))
                {
                    type = DataType.DateTimeOffset;
                }
                else if (!success)
                {
                    type = DataType.String;
                }

                fields.Add(new DataField(column.ColumnName, type));
            }

            return fields;
        }

        /// <summary>
        /// Generates a test data set
        /// </summary>
        /// <returns></returns>
        private static DataTable GenerateTestData()
        {
            var dt = new DataTable("demo");
            dt.Columns.AddRange(new[]
            {
                new DataColumn("id", typeof(int)),
                new DataColumn("name", typeof(string)),
                new DataColumn("age", typeof(int)),
                new DataColumn("lastseen", typeof(DateTime)),
                new DataColumn("score", typeof(double))
            });

            var rand = new Random();

            var forenames = new List<string> { "Celaena", "Gabe", "Rose", "Chris", "Girton", "Nick", "Aiana", "Julia", "Curtis", "Manuela", "Spike" };
            var surnames = new List<string> { "Smith", "Curts", "Power", "Stark", "Robinson", "Askew", "Maximus" };
            var ages = new List<int?> { 19, 23, 47, 71, null, 27 };
            var lastSeen = new List<DateTime> { new DateTime(2018, 9, 14), new DateTime(2019, 7, 13), new DateTime(1418, 5, 21) };
            var scores = Enumerable.Range(1, 10).Select(_ => rand.NextDouble()).ToList();

            var sampleData = from f in forenames
                             from s in surnames
                             from a in ages
                             from ls in lastSeen
                             from sc in scores
                             select new
                             {
                                 Name = $"{f} {s}",
                                 Age = a,
                                 LastSeen = ls,
                                 Score = sc
                             };

            var index = 1;

            foreach (var item in sampleData)
            {
                dt.Rows.Add(dt.NewRow().ItemArray = new object[] { index++, item.Name, item.Age, item.LastSeen, item.Score });
            }

            return dt;
        }
    }
}

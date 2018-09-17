# System.Data.DataTable to Parquet

Provides a simple console application to show how data can be transformed from a DataTable to Parquet using [Parquet.NET](https://github.com/elastacloud/parquet-dotnet).

There are times when you want to be able to write a query, get the results and persist them out without the need to create intermediate objects. This solution provides a way in which that can be accomplished.

This is by no means a complete solution and so I advise that you test it carefully before deploying to any production system. The solution also has the possibility of further optimisations which were ignored in this code base for the purposes of illustration and readability (is that actually a word?).

At the top of the `Program.cs` file there are 2 constants which may be changed to alter the output file path and the row group size. Currently the row group size is set to a small value for demonstration purposes, you may want to increase this but as noted in the [Parquet.NET documentation](https://github.com/elastacloud/parquet-dotnet/blob/master/doc/writing.md) you probably don't want to go above 5000.
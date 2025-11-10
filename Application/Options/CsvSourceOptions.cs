using System.Collections.Generic;

namespace Application.Options;

public sealed class CsvSourceOptions
{
    public const string SectionName = "CsvSource";

    public string DatasetName { get; set; } = "customers";

    /// <summary>
    /// Optional absolute or relative path to a single CSV file.
    /// </summary>
    public string? FilePath { get; set; }

    /// <summary>
    /// Optional absolute or relative directory that contains CSV files to process.
    /// </summary>
    public string? DirectoryPath { get; set; }

    /// <summary>
    /// File pattern to use when scanning <see cref="DirectoryPath"/>.
    /// </summary>
    public string SearchPattern { get; set; } = "*.csv";

    public char Delimiter { get; set; } = ',';

    /// <summary>
    /// Columns that must exist in each CSV file before it is processed.
    /// </summary>
    public IList<string> RequiredHeaders { get; set; } = new List<string>
    {
        "CustomerId",
        "FirstName",
        "LastName",
        "Email",
        "Phone",
        "City",
        "Country"
    };
}

namespace Application.Options;

public sealed class CsvSourceOptions
{
    public const string SectionName = "CsvSource";

    public string DatasetName { get; set; } = "customers";

    public string FilePath { get; set; } = string.Empty;

    public char Delimiter { get; set; } = ',';
}

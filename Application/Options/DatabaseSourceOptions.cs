namespace Application.Options;

public sealed class DatabaseSourceOptions
{
    public const string SectionName = "DatabaseSource";

    public string DatasetName { get; set; } = "order_details";

    public string ConnectionString { get; set; } = string.Empty;

    public string Query { get; set; } = string.Empty;
}

namespace Application.Options;

public sealed class ApiSourceOptions
{
    public const string SectionName = "ApiSource";

    public string DatasetName { get; set; } = "products";

    public string BaseAddress { get; set; } = string.Empty;

    public string Endpoint { get; set; } = string.Empty;

    public int TimeoutSeconds { get; set; } = 30;
}

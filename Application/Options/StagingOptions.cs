namespace Application.Options;

public sealed class StagingOptions
{
    public const string SectionName = "Staging";

    public string BasePath { get; set; } = Path.Combine(AppContext.BaseDirectory, "staging");
}

using Application.Extraction;
using Application.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Infrastructure.Staging;

public sealed class FileStagingWriter : IStagingWriter
{
    private readonly StagingOptions _options;
    private readonly ILogger<FileStagingWriter> _logger;
    private readonly JsonSerializerOptions _serializerOptions = new()
    {
        WriteIndented = true
    };

    public FileStagingWriter(IOptions<StagingOptions> options, ILogger<FileStagingWriter> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<string> WriteAsync<T>(string datasetName, IReadOnlyCollection<T> records, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(_options.BasePath);
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
        var fileName = $"{datasetName}_{timestamp}.json";
        var filePath = Path.Combine(_options.BasePath, fileName);

        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, records, _serializerOptions, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Staged {Count} records for {Dataset} at {Path}", records.Count, datasetName, filePath);
        return filePath;
    }
}

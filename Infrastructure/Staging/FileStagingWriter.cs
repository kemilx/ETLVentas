using Application.Extraction;
using Application.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.IO;
using System.Text.Json;

namespace Infrastructure.Staging;

public sealed class FileStagingWriter : IStagingWriter
{
    private readonly StagingOptions _options;
    private readonly ILogger<FileStagingWriter> _logger;
    private readonly IHostEnvironment _environment;
    private readonly JsonSerializerOptions _serializerOptions = new()
    {
        WriteIndented = true
    };

    public FileStagingWriter(
        IOptions<StagingOptions> options,
        ILogger<FileStagingWriter> logger,
        IHostEnvironment environment)
    {
        _options = options.Value;
        _logger = logger;
        _environment = environment;
    }

    public async Task<string> WriteAsync<T>(string datasetName, IReadOnlyCollection<T> records, CancellationToken cancellationToken)
    {
        var basePath = ResolveBasePath();
        Directory.CreateDirectory(basePath);
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
        var fileName = $"{datasetName}_{timestamp}.json";
        var filePath = Path.Combine(basePath, fileName);

        await using var stream = File.Create(filePath);
        await JsonSerializer.SerializeAsync(stream, records, _serializerOptions, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Staged {Count} records for {Dataset} at {Path}", records.Count, datasetName, filePath);
        return filePath;
    }

    private string ResolveBasePath()
    {
        var basePath = _options.BasePath;
        if (string.IsNullOrWhiteSpace(basePath))
        {
            return _environment.ContentRootPath;
        }

        return Path.IsPathRooted(basePath)
            ? basePath
            : Path.GetFullPath(Path.Combine(_environment.ContentRootPath, basePath));
    }
}

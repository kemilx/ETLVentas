using Application.Extraction;
using Application.Options;
using CsvHelper;
using CsvHelper.Configuration;
using Domain.Dtos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace Infrastructure.Extractors;

public sealed class CsvCustomerExtractor : IExtractor
{
    private readonly CsvSourceOptions _options;
    private readonly ILogger<CsvCustomerExtractor> _logger;

    public CsvCustomerExtractor(IOptions<CsvSourceOptions> options, ILogger<CsvCustomerExtractor> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public string Name => _options.DatasetName;

    public async Task ExtractAsync(IStagingWriter stagingWriter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.FilePath))
        {
            _logger.LogWarning("CSV source path is not configured");
            return;
        }

        if (!File.Exists(_options.FilePath))
        {
            _logger.LogWarning("CSV file {Path} was not found", _options.FilePath);
            return;
        }

        var records = new List<CustomerRecord>();
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter.ToString(),
            MissingFieldFound = null,
            HeaderValidated = null
        };

        await using var stream = File.OpenRead(_options.FilePath);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, configuration);

        await foreach (var record in csv.GetRecordsAsync<CustomerRecord>(cancellationToken))
        {
            records.Add(record);
        }

        _logger.LogInformation("CSV extraction produced {Count} records", records.Count);
        await stagingWriter.WriteAsync(_options.DatasetName, records, cancellationToken);
    }
}

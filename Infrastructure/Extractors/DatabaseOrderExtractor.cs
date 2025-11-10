using Application.Extraction;
using Application.Options;
using Domain.Dtos;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Infrastructure.Extractors;

public sealed class DatabaseOrderExtractor : IExtractor
{
    private readonly DatabaseSourceOptions _options;
    private readonly ILogger<DatabaseOrderExtractor> _logger;

    public DatabaseOrderExtractor(IOptions<DatabaseSourceOptions> options, ILogger<DatabaseOrderExtractor> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public string Name => _options.DatasetName;

    public async Task ExtractAsync(IStagingWriter stagingWriter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString) || string.IsNullOrWhiteSpace(_options.Query))
        {
            _logger.LogWarning("Database connection is not fully configured");
            return;
        }

        var records = new List<OrderDetailRecord>();

        try
        {
            await using var connection = new SqlConnection(_options.ConnectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            await using var command = new SqlCommand(_options.Query, connection)
            {
                CommandTimeout = 60
            };

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var order = new OrderDetailRecord(
                    reader.GetInt32(reader.GetOrdinal("OrderId")),
                    reader.GetInt32(reader.GetOrdinal("CustomerId")),
                    reader.GetInt32(reader.GetOrdinal("ProductId")),
                    reader.GetInt32(reader.GetOrdinal("Quantity")),
                    reader.GetDecimal(reader.GetOrdinal("TotalPrice")),
                    reader.GetDateTime(reader.GetOrdinal("OrderDate")));

                records.Add(order);
            }
        }
        catch (SqlException ex)
        {
            _logger.LogError(ex, "Database extraction failed for dataset {Dataset}", _options.DatasetName);
            return;
        }

        _logger.LogInformation("Database extraction produced {Count} records", records.Count);
        if (records.Count == 0)
        {
            _logger.LogWarning("Database extraction for {Dataset} produced no records", _options.DatasetName);
            return;
        }

        var stagedFile = await stagingWriter
            .WriteAsync(_options.DatasetName, records, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Database data for dataset {Dataset} was staged at {Path}",
            _options.DatasetName,
            stagedFile);
    }
}

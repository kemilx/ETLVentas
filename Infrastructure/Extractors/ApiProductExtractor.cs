using Application.Extraction;
using Application.Options;
using Domain.Dtos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Json;

namespace Infrastructure.Extractors;

public sealed class ApiProductExtractor : IExtractor
{
    private readonly HttpClient _httpClient;
    private readonly ApiSourceOptions _options;
    private readonly ILogger<ApiProductExtractor> _logger;

    public ApiProductExtractor(HttpClient httpClient, IOptions<ApiSourceOptions> options, ILogger<ApiProductExtractor> logger)
    {
        _httpClient = httpClient;
        _options = options.Value;
        _logger = logger;

        if (!string.IsNullOrWhiteSpace(_options.BaseAddress))
        {
            _httpClient.BaseAddress = new Uri(_options.BaseAddress);
        }

        _httpClient.Timeout = TimeSpan.FromSeconds(Math.Max(1, _options.TimeoutSeconds));
    }

    public string Name => _options.DatasetName;

    public async Task ExtractAsync(IStagingWriter stagingWriter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.Endpoint))
        {
            _logger.LogWarning("API endpoint is not configured");
            return;
        }

        try
        {
            var products = await _httpClient.GetFromJsonAsync<IReadOnlyCollection<ProductRecord>>(
                _options.Endpoint,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            if (products is null)
            {
                _logger.LogWarning("API response for {Endpoint} returned no data", _options.Endpoint);
                return;
            }

            _logger.LogInformation("API extraction produced {Count} records", products.Count);
            if (products.Count == 0)
            {
                _logger.LogWarning("API extraction for {Dataset} returned an empty collection", _options.DatasetName);
                return;
            }

            var stagedFile = await stagingWriter
                .WriteAsync(_options.DatasetName, products, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "API data for dataset {Dataset} was staged at {Path}",
                _options.DatasetName,
                stagedFile);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "API extraction failed for dataset {Dataset}", _options.DatasetName);
        }
    }
}

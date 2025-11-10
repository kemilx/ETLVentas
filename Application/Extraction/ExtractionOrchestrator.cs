using Microsoft.Extensions.Logging;

namespace Application.Extraction;

public sealed class ExtractionOrchestrator
{
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly IStagingWriter _stagingWriter;
    private readonly ILogger<ExtractionOrchestrator> _logger;

    public ExtractionOrchestrator(
        IEnumerable<IExtractor> extractors,
        IStagingWriter stagingWriter,
        ILogger<ExtractionOrchestrator> logger)
    {
        _extractors = extractors;
        _stagingWriter = stagingWriter;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var tasks = _extractors.Select(extractor => ExecuteExtractorAsync(extractor, cancellationToken)).ToList();
        if (tasks.Count == 0)
        {
            _logger.LogWarning("No extractors were registered. Skipping extraction cycle.");
            return;
        }

        await Task.WhenAll(tasks);
        _logger.LogInformation("All extractors finished their execution for this cycle.");
    }

    private async Task ExecuteExtractorAsync(IExtractor extractor, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Starting extraction for {Extractor}", extractor.Name);
            await extractor.ExtractAsync(_stagingWriter, cancellationToken);
            _logger.LogInformation("Extraction for {Extractor} completed", extractor.Name);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Extraction for {Extractor} canceled", extractor.Name);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Extraction for {Extractor} failed", extractor.Name);
        }
    }
}

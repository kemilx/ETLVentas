using System.Diagnostics;
using Application.Extraction;
using Microsoft.Extensions.Options;

namespace Worker
{
    public sealed class Worker : BackgroundService
    {
        private readonly ExtractionOrchestrator _orchestrator;
        private readonly IOptionsMonitor<WorkerOptions> _options;
        private readonly ILogger<Worker> _logger;

        public Worker(
            ExtractionOrchestrator orchestrator,
            IOptionsMonitor<WorkerOptions> options,
            ILogger<Worker> logger)
        {
            _orchestrator = orchestrator;
            _options = options;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var interval = TimeSpan.FromSeconds(Math.Max(1, _options.CurrentValue.IntervalSeconds));
                var stopwatch = Stopwatch.StartNew();

                _logger.LogInformation("Starting extraction cycle");
                await _orchestrator.RunAsync(stoppingToken);
                stopwatch.Stop();

                _logger.LogInformation("Extraction cycle completed in {Elapsed}", stopwatch.Elapsed);

                try
                {
                    await Task.Delay(interval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Host is shutting down; break the loop.
                    break;
                }
            }
        }
    }
}

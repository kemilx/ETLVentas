using Application.Extraction;
using Application.Options;
using CsvHelper;
using CsvHelper.Configuration;
using Domain.Dtos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace Infrastructure.Extractors;

public sealed class CsvCustomerExtractor : IExtractor
{
    private readonly CsvSourceOptions _options;
    private readonly ILogger<CsvCustomerExtractor> _logger;
    private readonly IHostEnvironment _environment;

    public CsvCustomerExtractor(
        IOptions<CsvSourceOptions> options,
        ILogger<CsvCustomerExtractor> logger,
        IHostEnvironment environment)
    {
        _options = options.Value;
        _logger = logger;
        _environment = environment;
    }

    public string Name => _options.DatasetName;

    public async Task ExtractAsync(IStagingWriter stagingWriter, CancellationToken cancellationToken)
    {
        var files = ResolveCandidateFiles();
        if (files.Count == 0)
        {
            _logger.LogWarning(
                "CSV extraction skipped because no files were discovered for dataset {Dataset}",
                _options.DatasetName);
            return;
        }

        var records = new List<CustomerRecord>();
        var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter.ToString(),
            MissingFieldFound = null,
            HeaderValidated = null
        };

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var stream = File.OpenRead(file);
            using var reader = new StreamReader(stream);
            using var csv = new CsvReader(reader, configuration);

            var fileCount = 0;
            await foreach (var record in csv.GetRecordsAsync<CustomerRecord>(cancellationToken))
            {
                records.Add(record);
                fileCount++;
            }

            _logger.LogInformation("Read {Count} records from CSV file {File}", fileCount, file);
        }

        _logger.LogInformation("CSV extraction produced {Count} records", records.Count);
        if (records.Count == 0)
        {
            _logger.LogWarning("No customer records were produced after reading {FileCount} files", files.Count);
            return;
        }

        await stagingWriter.WriteAsync(_options.DatasetName, records, cancellationToken);
    }

    private IReadOnlyList<string> ResolveCandidateFiles()
    {
        var discoveredFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(_options.FilePath))
        {
            var resolvedFile = ResolvePath(_options.FilePath);
            if (File.Exists(resolvedFile))
            {
                discoveredFiles.Add(resolvedFile);
            }
            else
            {
                _logger.LogWarning("Configured CSV file {Path} was not found", resolvedFile);
            }
        }

        if (!string.IsNullOrWhiteSpace(_options.DirectoryPath))
        {
            var resolvedDirectory = ResolvePath(_options.DirectoryPath);
            if (!Directory.Exists(resolvedDirectory))
            {
                _logger.LogWarning("Configured CSV directory {Directory} was not found", resolvedDirectory);
            }
            else
            {
                var searchPattern = string.IsNullOrWhiteSpace(_options.SearchPattern)
                    ? "*.csv"
                    : _options.SearchPattern;

                var matchingFiles = Directory.EnumerateFiles(resolvedDirectory, searchPattern, SearchOption.TopDirectoryOnly)
                    .ToList();

                if (matchingFiles.Count == 0)
                {
                    _logger.LogWarning(
                        "No CSV files in directory {Directory} matched pattern {Pattern}",
                        resolvedDirectory,
                        searchPattern);
                }
                else
                {
                    foreach (var file in matchingFiles)
                    {
                        discoveredFiles.Add(file);
                    }
                }
            }
        }

        return discoveredFiles.ToList();
    }

    private string ResolvePath(string path)
    {
        if (Path.IsPathRooted(path))
        {
            return path;
        }

        var basePath = _environment.ContentRootPath;
        return Path.GetFullPath(Path.Combine(basePath, path));
    }
}

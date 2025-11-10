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
using System.Text;

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
        var processedFiles = 0;

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!ValidateHeaders(file))
            {
                continue;
            }

            await using var stream = File.OpenRead(file);
            using var reader = new StreamReader(stream, Encoding.UTF8, true);
            using var csv = new CsvReader(reader, CreateConfiguration());

            var fileCount = 0;
            await foreach (var record in csv.GetRecordsAsync<CustomerRecord>(cancellationToken))
            {
                records.Add(record);
                fileCount++;
            }

            processedFiles++;
            _logger.LogInformation("Read {Count} records from CSV file {File}", fileCount, file);
        }

        _logger.LogInformation("CSV extraction produced {Count} records", records.Count);
        if (records.Count == 0)
        {
            _logger.LogWarning(
                "No customer records were produced after processing {FileCount} files. Review warnings above for skipped files.",
                processedFiles);
            return;
        }

        var stagedFile = await stagingWriter.WriteAsync(_options.DatasetName, records, cancellationToken);
        _logger.LogInformation(
            "CSV data for dataset {Dataset} was staged at {Path}",
            _options.DatasetName,
            stagedFile);
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

                _logger.LogInformation(
                    "Scanning CSV directory {Directory} using pattern {Pattern}",
                    resolvedDirectory,
                    searchPattern);

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

        _logger.LogInformation(
            "Discovered {Count} candidate CSV files for dataset {Dataset}",
            discoveredFiles.Count,
            _options.DatasetName);

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

    private CsvConfiguration CreateConfiguration()
    {
        return new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter.ToString(),
            MissingFieldFound = null,
            HeaderValidated = null
        };
    }

    private bool ValidateHeaders(string file)
    {
        if (_options.RequiredHeaders is not { Count: > 0 })
        {
            return true;
        }

        using var stream = File.OpenRead(file);
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        using var csv = new CsvReader(reader, CreateConfiguration());

        if (!csv.Read())
        {
            _logger.LogWarning("Skipping CSV file {File} because it is empty", file);
            return false;
        }

        csv.ReadHeader();
        var headerRecord = csv.HeaderRecord ?? Array.Empty<string>();
        var normalizedHeaders = headerRecord
            .Select(header => header?.Trim().ToLowerInvariant() ?? string.Empty)
            .Where(header => !string.IsNullOrEmpty(header))
            .ToArray();

        var expectedHeaders = _options.RequiredHeaders
            .Select(header => header?.Trim().ToLowerInvariant() ?? string.Empty)
            .Where(header => !string.IsNullOrEmpty(header))
            .ToArray();

        var missingHeaders = expectedHeaders
            .Where(expected => !normalizedHeaders.Contains(expected, StringComparer.OrdinalIgnoreCase))
            .ToArray();

        if (missingHeaders.Length == 0)
        {
            return true;
        }

        _logger.LogWarning(
            "Skipping CSV file {File} because it does not contain the required columns. Missing: {Missing}. Headers found: {Headers}",
            file,
            string.Join(", ", missingHeaders),
            string.Join(", ", headerRecord));

        return false;
    }
}

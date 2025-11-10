using Application.Extraction;
using Application.Options;
using Infrastructure.Extractors;
using Infrastructure.Staging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    options.SingleLine = true;
});
builder.Logging.AddDebug();

Console.WriteLine("Inicializando el servicio ETL... (revisa los logs para ver cada etapa)");

builder.Services.Configure<CsvSourceOptions>(builder.Configuration.GetSection(CsvSourceOptions.SectionName));
builder.Services.Configure<DatabaseSourceOptions>(builder.Configuration.GetSection(DatabaseSourceOptions.SectionName));
builder.Services.Configure<ApiSourceOptions>(builder.Configuration.GetSection(ApiSourceOptions.SectionName));
builder.Services.Configure<StagingOptions>(builder.Configuration.GetSection(StagingOptions.SectionName));
builder.Services.Configure<WorkerOptions>(builder.Configuration.GetSection(WorkerOptions.SectionName));

builder.Services.AddSingleton<IStagingWriter, FileStagingWriter>();
builder.Services.AddTransient<IExtractor, CsvCustomerExtractor>();
builder.Services.AddTransient<IExtractor, DatabaseOrderExtractor>();
builder.Services.AddHttpClient<ApiProductExtractor>();
builder.Services.AddTransient<IExtractor>(sp => sp.GetRequiredService<ApiProductExtractor>());

builder.Services.AddSingleton<ExtractionOrchestrator>();
builder.Services.AddHostedService<Worker.Worker>();

var host = builder.Build();
host.Run();

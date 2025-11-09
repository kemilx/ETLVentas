using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Application.Extraction;

public interface IStagingWriter
{
    Task<string> WriteAsync<T>(string datasetName, IReadOnlyCollection<T> records, CancellationToken cancellationToken);
}

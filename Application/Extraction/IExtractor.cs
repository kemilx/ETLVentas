using System.Threading;
using System.Threading.Tasks;

namespace Application.Extraction;

public interface IExtractor
{
    string Name { get; }

    Task ExtractAsync(IStagingWriter stagingWriter, CancellationToken cancellationToken);
}

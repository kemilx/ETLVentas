namespace Domain.Dtos;

public sealed record ProductRecord(
    int ProductId,
    string ProductName,
    string Category,
    decimal Price,
    int Stock);

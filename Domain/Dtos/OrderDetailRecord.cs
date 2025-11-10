namespace Domain.Dtos;

public sealed record OrderDetailRecord(
    int OrderId,
    int CustomerId,
    int ProductId,
    int Quantity,
    decimal TotalPrice,
    DateTime OrderDate);

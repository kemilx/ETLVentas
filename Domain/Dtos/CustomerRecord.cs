namespace Domain.Dtos;

public sealed record CustomerRecord(
    int CustomerId,
    string FirstName,
    string LastName,
    string Email,
    string Phone,
    string City,
    string Country);

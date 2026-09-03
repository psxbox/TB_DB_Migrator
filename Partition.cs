namespace TbMigrator;

public static class Partition
{
    public static long Compute(long tsMs, string strategy)
    {
        var dt = DateTimeOffset.FromUnixTimeMilliseconds(tsMs).UtcDateTime;
        DateTimeOffset t = strategy switch
        {
            "MINUTES"    => new DateTimeOffset(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, TimeSpan.Zero),
            "HOURS"      => new DateTimeOffset(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, TimeSpan.Zero),
            "DAYS"       => new DateTimeOffset(dt.Year, dt.Month, dt.Day, 0, 0, 0, TimeSpan.Zero),
            "MONTHS"     => new DateTimeOffset(dt.Year, dt.Month, 1, 0, 0, 0, TimeSpan.Zero),
            "YEARS"      => new DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
            "INDEFINITE" => DateTimeOffset.MinValue,
            _ => throw new ArgumentException($"Unknown partitioning: {strategy}")
        };
        return strategy == "INDEFINITE" ? 0 : t.ToUnixTimeMilliseconds();
    }
}

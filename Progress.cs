using System.Text.Json;
using System.Text.Json.Serialization;

namespace TbMigrator;

public class Progress
{
    [JsonPropertyName("phase")]              public string  Phase              { get; set; } = "init";
    [JsonPropertyName("started_at")]         public string  StartedAt          { get; set; } = "";
    [JsonPropertyName("partitioning")]       public string  Partitioning       { get; set; } = "MONTHS";
    [JsonPropertyName("cast_strings")]       public bool    CastStrings        { get; set; }
    [JsonPropertyName("phase1_start_ts")]    public long    Phase1StartTs      { get; set; }
    [JsonPropertyName("migrated_rows")]      public long    MigratedRows       { get; set; }
    [JsonPropertyName("skipped_rows")]       public long    SkippedRows        { get; set; }
    [JsonPropertyName("watermark_ts")]       public long    WatermarkTs        { get; set; }
    [JsonPropertyName("last_entity_id")]     public string? LastEntityId       { get; set; }
    [JsonPropertyName("completed_entities")] public List<string> CompletedEntities { get; set; } = [];
}

public class ProgressTracker
{
    private static readonly JsonSerializerOptions JsonOpts =
        new() { WriteIndented = true };

    private readonly string _file;
    private readonly object _lock = new();

    public Progress Progress { get; private set; } = new();

    public ProgressTracker(string file) => _file = file;

    public bool Load()
    {
        if (!File.Exists(_file)) return false;
        var json = File.ReadAllText(_file);
        Progress = JsonSerializer.Deserialize<Progress>(json) ?? new Progress();
        return true;
    }

    public void Update(Action<Progress> mutate)
    {
        lock (_lock)
        {
            mutate(Progress);
            File.WriteAllText(_file, JsonSerializer.Serialize(Progress, JsonOpts));
        }
    }
}

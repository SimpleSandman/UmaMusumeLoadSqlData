using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace UmaMusumeLoadSqlData.Models
{
    public class GithubRepoRoot
    {
        [JsonPropertyName("sha")]
        public string Sha { get; set; }

        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("tree")]
        public List<Tree> Trees { get; set; }

        [JsonPropertyName("truncated")]
        public bool Truncated { get; set; }
    }

    public class Tree
    {
        [JsonPropertyName("path")]
        public string Path { get; set; }

        [JsonPropertyName("mode")]
        public string Mode { get; set; }

        [JsonPropertyName("type")]
        public string Type { get; set; }

        [JsonPropertyName("sha")]
        public string Sha { get; set; }

        [JsonPropertyName("url")]
        public string Url { get; set; }

        [JsonPropertyName("size")]
        public int? Size { get; set; }
    }
}

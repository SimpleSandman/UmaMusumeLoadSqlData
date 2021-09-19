using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;

namespace UmaMusumeLoadSqlData.Utilities
{
    public static class GithubUtility
    {
        /// <summary>
        /// Download a raw file from remote GitHub repo
        /// </summary>
        /// <param name="repoName">Repository name (owner/repo name)</param>
        /// <param name="branchName">Branch name</param>
        /// <param name="sourceFilepath">File path to the remote file (path/to/file.txt)</param>
        /// <param name="destinationFilepath">File path for the destination file</param>
        public static void DownloadRemoteFile(string repoName, string branchName, string sourceFilepath, string destinationFilepath)
        {
            string rawUrl = $"https://raw.githubusercontent.com/{repoName}/{branchName}/{sourceFilepath}";

            Console.WriteLine($"\nDownloading \"{sourceFilepath}\" from \"{repoName}/{branchName}\"...");

            using (WebClient client = new WebClient())
            {
                client.Headers.Add("user-agent", "Anything"); // user agent is required https://developer.github.com/v3/#user-agent-required
                byte[] bytes = client.DownloadData(rawUrl);
                File.WriteAllBytes(destinationFilepath, bytes);
            }

            Console.WriteLine($"SUCCESS: Downloaded \"{sourceFilepath}\" to \"{destinationFilepath}\"");
        }

        /// <summary>
        /// Get responses from the GitHub API
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="uri"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public static async Task<T> GetGithubResponseAsync<T>(string uri, HttpClient client)
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.TryAddWithoutValidation("user-agent", "Anything"); // user agent is required https://developer.github.com/v3/#user-agent-required

            using (HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead))
            {
                if (response.IsSuccessStatusCode)
                {
                    try
                    {
                        return await response.Content.ReadFromJsonAsync<T>();
                    }
                    catch (NotSupportedException)
                    {
                        Console.WriteLine("\nFATAL ERROR: The content type is not supported");
                    }
                    catch (JsonException)
                    {
                        Console.WriteLine("\nFATAL ERROR: Invalid JSON");
                    }
                }
            }

            return default;
        }
    }
}

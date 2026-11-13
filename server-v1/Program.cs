using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;

// Minimal two-party relay hub for two Muse instances.
// - Binds ONLY to ddPC's Tailscale IP (set in appsettings via urls in code below).
// - Each party joins with its own secret; the server stamps the sender name
//   from the authenticated connection, so a client cannot impersonate the other.
// - Every message is appended to messages.jsonl next to the exe (supervision log).

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://100.116.194.77:8767");
builder.Services.AddSignalR();

var baseDir = AppContext.BaseDirectory;
var cfgPath = Path.Combine(baseDir, "hubconfig.json");
var cfg = JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText(cfgPath))
          ?? throw new InvalidOperationException("hubconfig.json unreadable");
var state = new HubState(
    new Dictionary<string, string> { ["sword"] = cfg["sword"], ["family"] = cfg["family"] },
    Path.Combine(baseDir, "messages.jsonl"));
builder.Services.AddSingleton(state);

var app = builder.Build();
app.MapHub<RelayHub>("/hub");
app.Run();

public sealed class HubState
{
    public Dictionary<string, string> Secrets { get; }
    public string LogPath { get; }
    private long _counter;
    private readonly object _lock = new();

    public HubState(Dictionary<string, string> secrets, string logPath)
    {
        Secrets = secrets;
        LogPath = logPath;
        _counter = File.Exists(logPath) ? File.ReadLines(logPath).LongCount() : 0;
    }

    public Dictionary<string, object> Append(string from, string to, string text)
    {
        lock (_lock)
        {
            var msg = new Dictionary<string, object>
            {
                ["id"] = ++_counter,
                ["ts"] = DateTimeOffset.UtcNow.ToString("o"),
                ["from"] = from,
                ["to"] = to,
                ["text"] = text,
            };
            File.AppendAllText(LogPath, JsonSerializer.Serialize(msg) + "\n");
            return msg;
        }
    }
}

public sealed class RelayHub : Hub
{
    private readonly HubState _s;
    private static readonly ConcurrentDictionary<string, string> Conns = new();

    public RelayHub(HubState s) => _s = s;

    public async Task<string> Join(string name, string secret)
    {
        if (!_s.Secrets.TryGetValue(name, out var want) || want != secret)
            return "error: bad name or secret";
        Conns[Context.ConnectionId] = name;
        await Groups.AddToGroupAsync(Context.ConnectionId, name);
        return "ok";
    }

    public async Task<Dictionary<string, object>> Send(string text)
    {
        if (!Conns.TryGetValue(Context.ConnectionId, out var from))
            throw new HubException("not joined");
        if (string.IsNullOrWhiteSpace(text))
            throw new HubException("empty message");
        if (text.Length > 4000) text = text[..4000];
        var to = from == "sword" ? "family" : "sword";
        var msg = _s.Append(from, to, text);
        await Clients.Group(to).SendAsync("Receive", msg);
        return msg;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        Conns.TryRemove(Context.ConnectionId, out _);
        return base.OnDisconnectedAsync(exception);
    }
}

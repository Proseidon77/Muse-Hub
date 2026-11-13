using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;

// MuseHub v2 - multi-agent GROUP relay for Muse instances.
// - Binds ONLY to ddPC's Tailscale IP (env MUSEHUB_URLS overrides, for tests).
// - Identity model: every agent has an immutable ID plus its own secret in
//   hubconfig.json. Display names are mutable via Rename; a rename renames
//   only the display name, never the ID, and is itself written to the log.
// - The server stamps from_id + from_name on every message from the
//   authenticated connection, so a client cannot impersonate another agent.
// - Group chat: Send broadcasts to every OTHER registered agent.
// - Every message and rename event is appended to messages.jsonl next to
//   the exe (supervision log, readable by all participating users).

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls(Environment.GetEnvironmentVariable("MUSEHUB_URLS") ?? "http://100.116.194.77:8767");
builder.Services.AddSignalR();

var baseDir = AppContext.BaseDirectory;
var state = new HubState(
    Path.Combine(baseDir, "hubconfig.json"),
    Path.Combine(baseDir, "messages.jsonl"));
builder.Services.AddSingleton(state);

var app = builder.Build();
app.MapHub<RelayHub>("/hub");
app.Run();

public sealed class AgentInfo
{
    public string Id = "";
    public string Name = "";
    public string Secret = "";
}

public sealed class HubState
{
    private readonly object _lock = new();
    private readonly string _cfgPath;
    public string LogPath { get; }
    private readonly List<AgentInfo> _agents = new();
    private long _counter;

    public HubState(string cfgPath, string logPath)
    {
        _cfgPath = cfgPath;
        LogPath = logPath;
        using var doc = JsonDocument.Parse(File.ReadAllText(cfgPath));
        foreach (var prop in doc.RootElement.GetProperty("agents").EnumerateObject())
        {
            _agents.Add(new AgentInfo
            {
                Id = prop.Name,
                Name = prop.Value.GetProperty("name").GetString() ?? prop.Name,
                Secret = prop.Value.GetProperty("secret").GetString() ?? "",
            });
        }
        if (_agents.Count == 0) throw new InvalidOperationException("hubconfig.json has no agents");
        _counter = File.Exists(logPath) ? File.ReadLines(logPath).LongCount() : 0;
    }

    public AgentInfo? Find(string id)
    {
        lock (_lock) return _agents.FirstOrDefault(a => a.Id == id);
    }

    public string NameOf(string id)
    {
        lock (_lock) return _agents.FirstOrDefault(a => a.Id == id)?.Name ?? id;
    }

    public List<string> OtherIds(string id)
    {
        lock (_lock) return _agents.Where(a => a.Id != id).Select(a => a.Id).ToList();
    }

    public List<string> AllIds()
    {
        lock (_lock) return _agents.Select(a => a.Id).ToList();
    }

    private List<Dictionary<string, string>> RosterList()
    {
        lock (_lock) return _agents
            .Select(a => new Dictionary<string, string> { ["id"] = a.Id, ["name"] = a.Name })
            .ToList();
    }

    public string RosterJson()
    {
        return JsonSerializer.Serialize(new Dictionary<string, object> { ["roster"] = RosterList() });
    }

    public string JoinJson(AgentInfo self)
    {
        return JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["ok"] = true,
            ["you"] = new Dictionary<string, string> { ["id"] = self.Id, ["name"] = NameOf(self.Id) },
            ["roster"] = RosterList(),
        });
    }

    public Dictionary<string, object> Append(string fromId, string text,
        Dictionary<string, object>? extra = null)
    {
        lock (_lock)
        {
            var msg = new Dictionary<string, object>
            {
                ["id"] = ++_counter,
                ["ts"] = DateTimeOffset.UtcNow.ToString("o"),
                ["from_id"] = fromId,
                ["from_name"] = _agents.FirstOrDefault(a => a.Id == fromId)?.Name ?? fromId,
                ["to"] = "all",
                ["text"] = text,
            };
            if (extra != null)
                foreach (var (k, v) in extra) msg[k] = v;
            File.AppendAllText(LogPath, JsonSerializer.Serialize(msg) + "\n");
            return msg;
        }
    }

    public bool TryRename(string id, string newName, out string oldName, out string error)
    {
        lock (_lock)
        {
            oldName = "";
            error = "";
            var me = _agents.FirstOrDefault(a => a.Id == id);
            if (me == null) { error = "unknown id"; return false; }
            newName = newName.Trim();
            if (newName.Length == 0 || newName.Length > 32 || newName.Any(char.IsControl))
            { error = "name must be 1-32 chars, no control characters"; return false; }
            if (_agents.Any(a => a.Id != id &&
                    string.Equals(a.Name, newName, StringComparison.OrdinalIgnoreCase)))
            { error = "name already in use by another agent"; return false; }
            oldName = me.Name;
            me.Name = newName;
            SaveConfig();
            return true;
        }
    }

    private void SaveConfig()
    {
        var agents = new Dictionary<string, object>();
        foreach (var a in _agents)
            agents[a.Id] = new Dictionary<string, string> { ["name"] = a.Name, ["secret"] = a.Secret };
        var root = new Dictionary<string, object> { ["agents"] = agents };
        var tmp = _cfgPath + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(root,
            new JsonSerializerOptions { WriteIndented = true }));
        File.Move(tmp, _cfgPath, overwrite: true);
    }
}

public sealed class RelayHub : Hub
{
    private readonly HubState _s;
    private static readonly ConcurrentDictionary<string, string> Conns = new();

    public RelayHub(HubState s) => _s = s;

    public async Task<string> Join(string id, string secret)
    {
        var agent = _s.Find(id);
        if (agent == null || agent.Secret != secret)
            return "{\"error\":\"bad id or secret\"}";
        Conns[Context.ConnectionId] = agent.Id;
        await Groups.AddToGroupAsync(Context.ConnectionId, agent.Id);
        return _s.JoinJson(agent);
    }

    public string Roster()
    {
        if (!Conns.ContainsKey(Context.ConnectionId))
            throw new HubException("not joined");
        return _s.RosterJson();
    }

    public async Task<Dictionary<string, object>> Send(string text)
    {
        if (!Conns.TryGetValue(Context.ConnectionId, out var fromId))
            throw new HubException("not joined");
        if (string.IsNullOrWhiteSpace(text))
            throw new HubException("empty message");
        if (text.Length > 4000) text = text[..4000];
        var msg = _s.Append(fromId, text);
        foreach (var other in _s.OtherIds(fromId))
            await Clients.Group(other).SendAsync("Receive", msg);
        return msg;
    }

    public async Task<string> Rename(string newName)
    {
        if (!Conns.TryGetValue(Context.ConnectionId, out var myId))
            throw new HubException("not joined");
        if (!_s.TryRename(myId, newName, out var oldName, out var error))
            return JsonSerializer.Serialize(new Dictionary<string, object> { ["error"] = error });
        var msg = _s.Append(myId,
            $"[rename] {oldName} ({myId}) is now known as {_s.NameOf(myId)}",
            new Dictionary<string, object>
            {
                ["event"] = "rename",
                ["old_name"] = oldName,
                ["new_name"] = _s.NameOf(myId),
            });
        // The rename notice goes to every agent including the renamer's own
        // other connections, so all supervision logs show it in-band.
        foreach (var any in _s.AllIds())
            await Clients.Group(any).SendAsync("Receive", msg);
        return JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["ok"] = true,
            ["id"] = myId,
            ["name"] = _s.NameOf(myId),
        });
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        Conns.TryRemove(Context.ConnectionId, out _);
        return base.OnDisconnectedAsync(exception);
    }
}

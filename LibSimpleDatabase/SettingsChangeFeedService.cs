using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace LibSimpleDatabase;

public interface ISettingsChangeFeed
{
    // Raised when a setting changes (key + op: 'I','U','D')
    event EventHandler<SettingChangedEventArgs> Changed;
}

public sealed class SettingChangedEventArgs : EventArgs
{
    public string Key { get; }
    public string Operation { get; } // 'I', 'U', 'D'
    public SettingChangedEventArgs(string key, string operation)
    {
        Key = key ?? string.Empty;
        Operation = operation ?? string.Empty;
    }
}

// Hosted service that runs the SimpleDatabaseChangeFeed and publishes events.
// This class is UI-agnostic; optionally set DeliveryContext to marshal events.
public sealed class SettingsChangeFeedService : IHostedService, ISettingsChangeFeed, IDisposable
{
    private readonly string _dbPath;
    private readonly TimeSpan _interval;
    private SimpleDatabaseChangeFeed? _feed;
    private IDisposable? _subscription;

    // Optional context for marshaling events (e.g., WPF UI thread context).
    public SynchronizationContext? DeliveryContext { get; set; }

    public event EventHandler<SettingChangedEventArgs>? Changed;

    public SettingsChangeFeedService(string dbPath, TimeSpan? pollInterval = null, bool replayExisting = false, SynchronizationContext? deliveryContext = null)
    {
        if (string.IsNullOrWhiteSpace(dbPath))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(dbPath));

        _dbPath = dbPath;
        _interval = pollInterval ?? TimeSpan.FromMilliseconds(300);
        DeliveryContext = deliveryContext;
        _feed = new SimpleDatabaseChangeFeed(_dbPath, _interval, replayExisting);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_feed == null)
            _feed = new SimpleDatabaseChangeFeed(_dbPath, _interval);

        _subscription = _feed.Start(OnChange, cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        try { _subscription?.Dispose(); } catch { }
        _subscription = null;
        return Task.CompletedTask;
    }

    private void OnChange(string key, string op)
    {
        var args = new SettingChangedEventArgs(key, op);

        var ctx = DeliveryContext;
        if (ctx != null)
        {
            ctx.Post(_ => Changed?.Invoke(this, args), null);
        }
        else
        {
            Changed?.Invoke(this, args);
        }
    }

    public void Dispose()
    {
        try { _subscription?.Dispose(); } catch { }
        _subscription = null;
        _feed?.Dispose();
        _feed = null;
    }
}
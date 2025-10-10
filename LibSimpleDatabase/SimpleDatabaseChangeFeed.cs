using Logging.lib;
using SQLite;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LibSimpleDatabase;
// Polling-based change feed for SimpleSetting changes logged by triggers above.
public sealed class SimpleDatabaseChangeFeed : IDisposable
{
    private readonly string _dbFilePath;
    private readonly TimeSpan _interval;
    private long _lastId;
    private CancellationTokenSource? _cts;
    private Task? _loop;

    public SimpleDatabaseChangeFeed(string dbFilePath, TimeSpan? pollInterval = null, bool replayExisting = false)
    {
        if (string.IsNullOrWhiteSpace(dbFilePath))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(dbFilePath));

        _dbFilePath = dbFilePath;
        _interval = pollInterval ?? TimeSpan.FromMilliseconds(500);

        using var conn = new SQLite.SQLiteConnection(_dbFilePath);
        EnsureWalMode(conn);

        // Ensure change tracking objects exist even if no writer has opened the DB yet.
        EnsureChangeTrackingObjects(conn);

        _lastId = replayExisting ? 0 : GetMaxChangeId(conn);
    }

    public IDisposable Start(Action<string, string> onChange, CancellationToken cancellationToken = default)
    {
        if (onChange == null) throw new ArgumentNullException(nameof(onChange));

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = Task.Run(() => Loop(onChange, _cts.Token), _cts.Token);
        return new AnonymousDisposable(() =>
        {
            try { _cts?.Cancel(); } catch { }
            try { _loop?.Wait(1000); } catch { }
            _cts?.Dispose();
        });
    }

    private void EnsureWalMode(SQLiteConnection conn)
    {
        try
        {
            var current = conn.ExecuteScalar<string>("PRAGMA journal_mode;");
            if (!string.Equals(current, "wal", StringComparison.OrdinalIgnoreCase))
            {
                // This returns the new mode (e.g., "wal"); use Scalar, not Execute.
                var newMode = conn.ExecuteScalar<string>("PRAGMA journal_mode=WAL;");
                // Optional: verify
                // Log.Debug("SQLite journal_mode changed from {current} to {newMode}", current, newMode);
            }
        }
        catch (SQLiteException ex)
        {
            // Some builds return “not an error” when PRAGMA returns a row via Execute.
            // We’re already using ExecuteScalar, but in case of any platform quirk, just log and continue.
            Logger.Debug(ex, "SQLiteException while setting WAL mode. Continuing with current journal_mode.");
            Logger.Debug("Unable to set WAL mode. Continuing with current journal_mode.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex, "Unexpected error while ensuring WAL mode.");
        }
    }


    private void Loop(Action<string, string> onChange, CancellationToken token)
    {
        using var conn = new SQLite.SQLiteConnection(_dbFilePath);
        while (!token.IsCancellationRequested)
        {
            // Read new changes since last Id
            var rows = conn.CreateCommand(
                "select Id, Key, Op from SimpleChange where Id > ? order by Id asc", _lastId)
                .ExecuteQuery<SimpleChangeRow>();

            foreach (var r in rows)
            {
                _lastId = r.Id;
                onChange(r.Key ?? string.Empty, r.Op ?? string.Empty);
            }

            token.WaitHandle.WaitOne(_interval);
        }
    }

    private static long GetMaxChangeId(SQLite.SQLiteConnection conn)
        => conn.CreateCommand("select ifnull(max(Id), 0) from SimpleChange").ExecuteScalar<long>();

    private static void EnsureChangeTrackingObjects(SQLite.SQLiteConnection conn)
    {
        // Mirror of the writer-side setup to be safe if only readers start first.
        conn.Execute(@"
            CREATE TABLE IF NOT EXISTS SimpleChange
            (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Key         TEXT,
                Op          TEXT,
                UnixTimeMs  INTEGER NOT NULL
            );");

        conn.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AI
            AFTER INSERT ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (NEW.Key, 'I', (strftime('%s','now')*1000));
            END;");

        conn.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AU
            AFTER UPDATE ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (NEW.Key, 'U', (strftime('%s','now')*1000));
            END;");

        conn.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AD
            AFTER DELETE ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (OLD.Key, 'D', (strftime('%s','now')*1000));
            END;");
    }

    private sealed class SimpleChangeRow
    {
        [SQLite.PrimaryKey]
        public long Id { get; set; }
        public string? Key { get; set; }
        public string? Op { get; set; }
    }

    private sealed class AnonymousDisposable : IDisposable
    {
        private Action? _dispose;
        public AnonymousDisposable(Action dispose) => _dispose = dispose;
        public void Dispose() => Interlocked.Exchange(ref _dispose, null)?.Invoke();
    }

    public void Dispose()
    {
        try { _cts?.Cancel(); } catch { }
        try { _loop?.Wait(1000); } catch { }
        _cts?.Dispose();
    }
}

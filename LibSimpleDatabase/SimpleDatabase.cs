using CommunityToolkit.Mvvm.ComponentModel;
using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Text;
using System.Linq;
using System.IO;
using Logging.lib;

namespace LibSimpleDatabase;

public class SimpleDatabase : ObservableObject, IDisposable
{
    public class SimpleSetting
    {
        [PrimaryKey]
        public string? Key { get; set; } = string.Empty;
        public string? Value { get; set; } = string.Empty;
    }

    private readonly string _prefix = string.Empty;
    private readonly string _suffix = string.Empty;

    private SQLiteConnection? Connection { get; set; } = null;
    private readonly object _sync = new();
    private string _dbFilePath = string.Empty;

    public SimpleDatabase(string? prefix = "", string? suffix = "")
    {
        _prefix = prefix ?? string.Empty;
        _suffix = suffix ?? string.Empty;
    }

    private string GetStorageKey(string key, bool useRawKey) => useRawKey ? key : string.Concat(_prefix, key, _suffix);

    public bool IsOpen => Connection != null && !string.IsNullOrEmpty(_dbFilePath);

    public bool Open(string dbFilePath)
    {
        if (string.IsNullOrEmpty(dbFilePath))
            return false;

        try
        {
            var dir = Path.GetDirectoryName(dbFilePath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            lock (_sync)
            {
                if (Connection != null)
                    Close();

                Connection = new SQLiteConnection(dbFilePath);

                // Set WAL using ExecuteScalar to avoid the "not an error" exception.
                EnsureWalMode(Connection);

                _ = Connection.CreateTable<SimpleSetting>();
                EnsureChangeTrackingObjects();
            }

            _dbFilePath = dbFilePath;
            return true;
        }
        catch (Exception ex)
        {
            try { Connection?.Dispose(); } catch { }
            Connection = null;
            _dbFilePath = string.Empty;
            Logger.Error(ex, "Failed to open SimpleDatabase at {dbFilePath}", dbFilePath);
            return false;
        }
    }

    private static void EnsureWalMode(SQLiteConnection conn)
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

    private void EnsureChangeTrackingObjects()
    {
        // Change log table
        _ = Connection?.Execute(@"
            CREATE TABLE IF NOT EXISTS SimpleChange
            (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Key         TEXT,
                Op          TEXT,     -- 'I','U','D'
                UnixTimeMs  INTEGER NOT NULL
            );");

        // Triggers to append to change log on any write to SimpleSetting
        _ = Connection?.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AI
            AFTER INSERT ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (NEW.Key, 'I', (strftime('%s','now')*1000));
            END;");

        _ = Connection?.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AU
            AFTER UPDATE ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (NEW.Key, 'U', (strftime('%s','now')*1000));
            END;");

        _ = Connection?.Execute(@"
            CREATE TRIGGER IF NOT EXISTS trg_SimpleSetting_AD
            AFTER DELETE ON SimpleSetting
            BEGIN
                INSERT INTO SimpleChange(Key, Op, UnixTimeMs)
                VALUES (OLD.Key, 'D', (strftime('%s','now')*1000));
            END;");
    }

    public static SimpleDatabase? Create(string dbFilePath)
    {
        SimpleDatabase db = new();
        return db.Open(dbFilePath) ? db : null;
    }

    public static SimpleDatabase? Create(string dbFilePath, string? prefix, string? suffix)
    {
        SimpleDatabase db = new(prefix, suffix);
        return db.Open(dbFilePath) ? db : null;
    }

    public T? GetValue<T>(string key, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false, bool useRawKey = false)
    {
        var settings = SelectSetting(key, useRawKey);
        if (settings == null)
        {
            if (setDefault)
                SetValue<T>(key, default, handling, noErrors, useRawKey);

            return default;
        }
        else
        {
            if (typeof(T) == typeof(string) || typeof(T).IsPrimitive)
            {
                return (T)Convert.ChangeType(settings.Value, typeof(T));
            }
            else
            {
                JsonSerializerSettings ser = new()
                {
                    TypeNameHandling = handling
                };

                if (noErrors)
                    ser.Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true;

                return string.IsNullOrEmpty(settings.Value) ? default : (T?)JsonConvert.DeserializeObject(settings.Value, typeof(T), ser);
            }
        }
    }

    public T? GetValue<T>(string key, T? defaultValue, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false, bool useRawKey = false)
    {
        var settings = SelectSetting(key, useRawKey);
        if (settings == null)
        {
            if (setDefault)
                SetValue<T>(key, defaultValue, handling, noErrors, useRawKey);

            return defaultValue;
        }
        else
        {
            if (typeof(T) == typeof(string) || typeof(T).IsPrimitive)
            {
                return (T)Convert.ChangeType(settings.Value, typeof(T));
            }
            else
            {
                JsonSerializerSettings ser = new()
                {
                    TypeNameHandling = handling
                };

                if (noErrors)
                    ser.Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true;

                return string.IsNullOrEmpty(settings.Value) ? default : (T?)JsonConvert.DeserializeObject(settings.Value, typeof(T), ser);
            }
        }
    }

    public List<T> GetAllValues<T>(TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false)
    {
        List<T> lst = [];

        var settings = SelectAllSettings();
        if (settings == null)
            return lst;

        JsonSerializerSettings ser = new()
        {
            TypeNameHandling = handling
        };

        if (noErrors)
            ser.Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true;

        foreach (var ss in settings)
        {
            var res = string.IsNullOrEmpty(ss.Value) ? default : (T?)JsonConvert.DeserializeObject(ss.Value, typeof(T), ser);
            if (res != null)
                lst.Add(res);
        }
        return lst;
    }

    // Left for compatibility
    public void SetValue(string key, string? value)
    {
        // Use the same keying scheme as other APIs (respects prefix/suffix).
        SetValue(key, value, useRawKey: false);
    }

    public void SetValue(string key, string? value, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);

        SimpleSetting set = new()
        {
            Key = storageKey,
            Value = value
        };
        _ = InsertOrReplace(set);

        OnPropertyChanged(key);
    }

    public void SetValue<T>(string key, T? value, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);

        if (typeof(T) == typeof(string) || typeof(T).IsPrimitive)
        {
            SimpleSetting set = new()
            {
                Key = storageKey,
                Value = value?.ToString()
            };
            _ = InsertOrReplace(set);
        }
        else
        {
            JsonSerializerSettings ser = new()
            {
                TypeNameHandling = handling
            };

            if (noErrors)
                ser.Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true;

            SimpleSetting set = new()
            {
                Key = storageKey,
                Value = JsonConvert.SerializeObject(value, ser)
            };
            _ = InsertOrReplace(set);
        }

        OnPropertyChanged(key);
    }

    private int? InsertOrReplace(SimpleSetting setting)
    {
        return ExecuteWrite(conn =>
        {
            int res = 0;
            conn.RunInTransaction(() =>
            {
                res = conn.InsertOrReplace(setting);
            });
            return (int?)res;
        });
    }


    public bool ExistsSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        var exists = ExecuteRead(conn => conn.Find<SimpleSetting>(storageKey) != null);
        return exists == true;
    }

    public SimpleSetting? SelectSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        return ExecuteRead(conn => conn.Find<SimpleSetting>(storageKey));
    }


    public List<SimpleSetting> SelectAllSettings()
    {
        var result = ExecuteRead(conn => conn.Query<SimpleSetting>("select Key, Value from SimpleSetting"));
        return result ?? new List<SimpleSetting>();
    }

    public List<SimpleSetting> SelectSettingsByKeys(IEnumerable<string> keys, bool useRawKey = false)
    {
        var storageKeys = keys?.Select(k => GetStorageKey(k, useRawKey)).Where(k => !string.IsNullOrEmpty(k)).ToArray() ?? Array.Empty<string>();
        if (storageKeys.Length == 0) return new List<SimpleSetting>();

        var placeholders = string.Join(",", Enumerable.Repeat("?", storageKeys.Length));
        return ExecuteRead(conn => conn.Query<SimpleSetting>($"select Key, Value from SimpleSetting where Key in ({placeholders})", storageKeys))
               ?? new List<SimpleSetting>();
    }

    public int? DeleteSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        var ret = Connection?.Table<SimpleSetting>().Delete(v => v.Key == storageKey);
        OnPropertyChanged(key);
        return ret;
    }

    public void Close()
    {
        Connection?.Dispose();
        Connection = null;
        _dbFilePath = string.Empty;
    }

    public void Dispose()
    {
        Connection?.Close();
        Connection?.Dispose();

        GC.SuppressFinalize(this);
    }

    private static string GetGlobalMutexName(string dbPath)
    {
        // Stable, short, cross-session name derived from the path.
        using var sha = SHA256.Create();
        var hex = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(dbPath))).Replace("-", "");
        return @"Global\LibSimpleDatabase_WriteLock_" + hex;
    }

    private static bool IsBusy(SQLiteException ex)
        => ex.Result == SQLite3.Result.Busy || ex.Result == SQLite3.Result.Locked;

    private static T ExecuteWithRetry<T>(Func<T> action, int maxRetries = 5, int initialDelayMs = 25)
    {
        var delay = initialDelayMs;
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return action();
            }
            catch (SQLiteException ex) when (IsBusy(ex) && attempt < maxRetries)
            {
                Thread.Sleep(delay);
                delay = Math.Min(delay * 2, 1000); // exponential backoff (cap at 1s)
            }
        }
    }

    private T? ExecuteWrite<T>(Func<SQLiteConnection, T?> func)
    {
        if (!IsOpen) throw new InvalidOperationException("SimpleDatabase is not open. Call Open(...) before writing.");

        var mutexName = GetGlobalMutexName(_dbFilePath);
        using var mutex = new Mutex(false, mutexName);

        bool lockTaken = false;
        try
        {
            lockTaken = mutex.WaitOne(TimeSpan.FromSeconds(10));
            return ExecuteWithRetry(() =>
            {
                lock (_sync)
                {
                    return func(Connection!);
                }
            });
        }
        finally
        {
            if (lockTaken)
            {
                try { mutex.ReleaseMutex(); } catch { }
            }
        }
    }

    private TOut? ExecuteRead<TOut>(Func<SQLiteConnection, TOut?> func)
    {
        if (!IsOpen) throw new InvalidOperationException("SimpleDatabase is not open. Call Open(...) before reading.");

        using var conn = new SQLiteConnection(_dbFilePath, SQLiteOpenFlags.ReadOnly);
        try
        {
            // Prefer API over PRAGMA for busy timeout to avoid result-row issues
            conn.BusyTimeout = TimeSpan.FromSeconds(5);

            // PRAGMAs that may return a row: use ExecuteScalar and ignore failures
            TrySetPragmaScalarLong(conn, "PRAGMA cache_size=-32768;");   // ~32 MB page cache (negative is KiB)
            TrySetPragmaScalarLong(conn, "PRAGMA mmap_size=134217728;"); // 128 MB mmap (best effort)
        }
        catch (Exception ex)
        {
            Logger.Debug(ex, "Error configuring read-only connection.");
        }

        return func(conn);
    }

    // Helper for PRAGMAs that return a numeric value after setting
    private static void TrySetPragmaScalarLong(SQLiteConnection conn, string sql)
    {
        try
        {
            _ = conn.ExecuteScalar<long>(sql);
        }
        catch (SQLiteException ex)
        {
            // Some platforms/SQLite builds may not support the pragma; ignore.
            Logger.Debug(ex, "PRAGMA failed (ignored): {sql}", sql);
        }
        catch (Exception ex)
        {
            Logger.Debug(ex, "Unexpected error setting PRAGMA (ignored): {sql}", sql);
        }
    }
}


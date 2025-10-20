using CommunityToolkit.Mvvm.ComponentModel;
using Logging.lib;
using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace LibSimpleDatabase;

/// <summary>Lightweight key-value store over SQLite with optional key prefix/suffix, change tracking, and concurrency safeguards.</summary>
public class SimpleDatabase : ObservableObject, IDisposable
{
    #region Nested types

    /// <summary>Represents a key-value row stored in the database.</summary>
    public class SimpleSetting
    {
        /// <summary>Gets or sets the unique key for the setting.</summary>
        [PrimaryKey]
        public string Key { get; set; } = string.Empty;

        /// <summary>Gets or sets the serialized value for the setting.</summary>
        public string? Value { get; set; } = string.Empty;
    }

    #endregion

    #region Fields

    /// <summary>Stores the key prefix applied unless raw keys are used via <see cref="GetStorageKey(string, bool)"/>.</summary>
    private readonly string _prefix = string.Empty;

    /// <summary>Stores the key suffix applied unless raw keys are used via <see cref="GetStorageKey(string, bool)"/>.</summary>
    private readonly string _suffix = string.Empty;

    /// <summary>Gets or sets the open SQLite connection for read-write operations.</summary>
    private SQLiteConnection? Connection { get; set; } = null;

    /// <summary>Serializes in-process access to <see cref="Connection"/>.</summary>
    private readonly object _sync = new();

    /// <summary>Holds the database file path for creating read-only connections and naming the global mutex.</summary>
    private string _dbFilePath = string.Empty;

    #endregion

    #region Constructors

    /// <summary>Initializes a new instance with an optional key prefix and suffix.</summary>
    public SimpleDatabase(string? prefix = "", string? suffix = "")
    {
        _prefix = prefix ?? string.Empty;
        _suffix = suffix ?? string.Empty;
    }

    #endregion

    #region Properties

    /// <summary>Gets a value indicating whether the database is open; true when <see cref="Connection"/> is initialized and <see cref="_dbFilePath"/> is not empty.</summary>
    public bool IsOpen => Connection != null && !string.IsNullOrEmpty(_dbFilePath);

    #endregion

    #region Factory

    /// <summary>Creates and opens a database at the specified file path.</summary>
    public static SimpleDatabase? Create(string dbFilePath)
    {
        SimpleDatabase db = new();
        return db.Open(dbFilePath) ? db : null;
    }

    /// <summary>Creates and opens a database at the specified path with a key prefix and suffix.</summary>
    public static SimpleDatabase? Create(string dbFilePath, string? prefix, string? suffix)
    {
        SimpleDatabase db = new(prefix, suffix);
        return db.Open(dbFilePath) ? db : null;
    }

    #endregion

    #region Open/Close/Dispose

    /// <summary>Opens or creates the database at the given path, enables WAL via <see cref="EnsureWalMode(SQLiteConnection)"/>, and ensures change tracking with <see cref="EnsureChangeTrackingObjects"/>.</summary>
    public bool Open(string dbFilePath)
    {
        if (string.IsNullOrEmpty(dbFilePath))
            return false;

        try
        {
            var dir = Path.GetDirectoryName(dbFilePath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                _ = Directory.CreateDirectory(dir);

            lock (_sync)
            {
                if (Connection != null)
                    Close();

                Connection = new SQLiteConnection(dbFilePath);

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

    /// <summary>Closes the open connection and resets the file path.</summary>
    public void Close()
    {
        Connection?.Dispose();
        Connection = null;
        _dbFilePath = string.Empty;
    }

    /// <summary>Disposes the connection and suppresses finalization.</summary>
    public void Dispose()
    {
        Connection?.Close();
        Connection?.Dispose();

        GC.SuppressFinalize(this);
    }

    #endregion

    #region CRUD API

    /// <summary>Gets a value for the specified key; optionally sets a default using <see cref="SetValue{T}(string, T, TypeNameHandling, bool, bool)"/> if not found.</summary>
    public T? GetValue<T>(string key, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false, bool useRawKey = false)
    {
        SimpleSetting? settings = SelectSetting(key, useRawKey);
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

    /// <summary>Gets a value for the specified key with a provided default; optionally writes that default via <see cref="SetValue{T}(string, T, TypeNameHandling, bool, bool)"/> when missing.</summary>
    public T? GetValue<T>(string key, T? defaultValue, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false, bool useRawKey = false)
    {
        SimpleSetting? settings = SelectSetting(key, useRawKey);
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

    /// <summary>Gets all stored values and deserializes them as instances of <typeparamref name="T"/>.</summary>
    public List<T> GetAllValues<T>(TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false)
    {
        List<T> lst = [];

        List<SimpleSetting> settings = SelectAllSettings();
        if (settings == null)
            return lst;

        JsonSerializerSettings ser = new()
        {
            TypeNameHandling = handling
        };

        if (noErrors)
            ser.Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true;

        foreach (SimpleSetting ss in settings)
        {
            T? res = string.IsNullOrEmpty(ss.Value) ? default : (T?)JsonConvert.DeserializeObject(ss.Value, typeof(T), ser);
            if (res != null)
                lst.Add(res);
        }
        return lst;
    }

    /// <summary>Sets a raw string value for the specified key.</summary>
    public void SetValue(string key, string? value) => SetValue(key, value, useRawKey: false);

    /// <summary>Sets a raw string value for the specified key with optional raw-key behavior.</summary>
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

    /// <summary>Serializes and stores the value for the specified key; primitives and strings are stored directly.</summary>
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

    /// <summary>Deletes the setting for the specified key and raises a change notification.</summary>
    public int? DeleteSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        var ret = Connection?.Table<SimpleSetting>().Delete(v => v.Key == storageKey);
        OnPropertyChanged(key);
        return ret;
    }

    #endregion

    #region Query API

    /// <summary>Determines whether a setting exists for the specified key.</summary>
    public bool ExistsSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        var exists = ExecuteRead(conn => conn.Find<SimpleSetting>(storageKey) != null);
        return exists == true;
    }

    /// <summary>Selects a single setting by key, or null if not found.</summary>
    public SimpleSetting? SelectSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        return ExecuteRead(conn => conn.Find<SimpleSetting>(storageKey));
    }

    /// <summary>Returns all settings as key-value rows.</summary>
    public List<SimpleSetting> SelectAllSettings()
    {
        List<SimpleSetting>? result = ExecuteRead(conn => conn.Query<SimpleSetting>("select Key, Value from SimpleSetting"));
        return result ?? [];
    }

    /// <summary>Returns settings for the specified keys, applying the current keying scheme.</summary>
    public List<SimpleSetting> SelectSettingsByKeys(IEnumerable<string> keys, bool useRawKey = false)
    {
        var storageKeys = keys?.Select(k => GetStorageKey(k, useRawKey)).Where(k => !string.IsNullOrEmpty(k)).ToArray() ?? Array.Empty<string>();
        if (storageKeys.Length == 0) return [];

        var placeholders = string.Join(",", Enumerable.Repeat("?", storageKeys.Length));
        return ExecuteRead(conn => conn.Query<SimpleSetting>($"select Key, Value from SimpleSetting where Key in ({placeholders})", storageKeys))
               ?? [];
    }

    #endregion

    #region Keying

    /// <summary>Builds the storage key using the configured prefix and suffix unless <paramref name="useRawKey"/> is true.</summary>
    private string GetStorageKey(string key, bool useRawKey) => useRawKey ? key : string.Concat(_prefix, key, _suffix);

    #endregion

    #region Write helpers

    /// <summary>Inserts or replaces the specified row within a write transaction using <see cref="ExecuteWrite{T}(Func{SQLiteConnection, T})"/>.</summary>
    private int? InsertOrReplace(SimpleSetting setting) => ExecuteWrite(conn =>
                                                                {
                                                                    var res = 0;
                                                                    conn.RunInTransaction(() => res = conn.InsertOrReplace(setting));
                                                                    return (int?)res;
                                                                });

    /// <summary>Performs a write under a cross-process mutex and in-process lock with retry logic via <see cref="ExecuteWithRetry{T}(Func{T})"/> and <see cref="GetGlobalMutexName(string)"/>.</summary>
    private T? ExecuteWrite<T>(Func<SQLiteConnection, T?> func)
    {
        if (!IsOpen) throw new InvalidOperationException("SimpleDatabase is not open. Call Open(...) before writing.");

        var mutexName = GetGlobalMutexName(_dbFilePath);
        using var mutex = new Mutex(false, mutexName);

        var lockTaken = false;
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

    #endregion

    #region Read helpers

    /// <summary>Executes a read operation using a temporary read-only connection with a busy timeout and best-effort PRAGMA tuning.</summary>
    private TOut? ExecuteRead<TOut>(Func<SQLiteConnection, TOut?> func)
    {
        if (!IsOpen) throw new InvalidOperationException("SimpleDatabase is not open. Call Open(...) before reading.");

        using var conn = new SQLiteConnection(_dbFilePath, SQLiteOpenFlags.ReadOnly);
        try
        {
            conn.BusyTimeout = TimeSpan.FromSeconds(5);
            TrySetPragmaScalarLong(conn, "PRAGMA cache_size=-32768;");
            TrySetPragmaScalarLong(conn, "PRAGMA mmap_size=134217728;");
        }
        catch (Exception ex)
        {
            Logger.Debug(ex, "Error configuring read-only connection.");
        }

        return func(conn);
    }

    #endregion

    #region Concurrency and retry

    /// <summary>Computes a stable global mutex name from the database path.</summary>
    private static string GetGlobalMutexName(string dbPath)
    {
        using var sha = SHA256.Create();
        var hex = BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(dbPath))).Replace("-", "");
        return @"Global\LibSimpleDatabase_WriteLock_" + hex;
    }

    /// <summary>Returns true when the SQLite exception indicates a busy or locked result.</summary>
    private static bool IsBusy(SQLiteException ex)
        => ex.Result is SQLite3.Result.Busy or SQLite3.Result.Locked;

    /// <summary>Executes the specified action with retries and exponential backoff when <see cref="IsBusy(SQLiteException)"/> conditions occur.</summary>
    private static T ExecuteWithRetry<T>(Func<T> action, int maxRetries = 5, int initialDelayMs = 25)
    {
        var delay = initialDelayMs;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return action();
            }
            catch (SQLiteException ex) when (IsBusy(ex) && attempt < maxRetries)
            {
                Thread.Sleep(delay);
                delay = Math.Min(delay * 2, 1000);
            }
        }
    }

    #endregion

    #region SQLite configuration

    /// <summary>Ensures the database uses Write-Ahead Logging (WAL) journal mode.</summary>
    private static void EnsureWalMode(SQLiteConnection conn)
    {
        try
        {
            var current = conn.ExecuteScalar<string>("PRAGMA journal_mode;");
            if (!string.Equals(current, "wal", StringComparison.OrdinalIgnoreCase))
            {
                var newMode = conn.ExecuteScalar<string>("PRAGMA journal_mode=WAL;");
            }
        }
        catch (SQLiteException ex)
        {
            Logger.Debug(ex, "SQLiteException while setting WAL mode. Continuing with current journal_mode.");
            Logger.Debug("Unable to set WAL mode. Continuing with current journal_mode.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex, "Unexpected error while ensuring WAL mode.");
        }
    }

    /// <summary>Attempts to set a PRAGMA that returns a numeric value and ignores failures.</summary>
    private static void TrySetPragmaScalarLong(SQLiteConnection conn, string sql)
    {
        try
        {
            _ = conn.ExecuteScalar<long>(sql);
        }
        catch (SQLiteException ex)
        {
            Logger.Debug(ex, "PRAGMA failed (ignored): {sql}", sql);
        }
        catch (Exception ex)
        {
            Logger.Debug(ex, "Unexpected error setting PRAGMA (ignored): {sql}", sql);
        }
    }

    #endregion

    #region Change tracking

    /// <summary>Creates the change log table and triggers that capture inserts, updates, and deletions.</summary>
    private void EnsureChangeTrackingObjects()
    {
        _ = Connection?.Execute(@"
            CREATE TABLE IF NOT EXISTS SimpleChange
            (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Key         TEXT,
                Op          TEXT,
                UnixTimeMs  INTEGER NOT NULL
            );");

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

    #endregion
}
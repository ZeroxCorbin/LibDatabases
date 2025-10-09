using CommunityToolkit.Mvvm.ComponentModel;
using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;

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

    public SimpleDatabase(string? prefix = "", string? suffix = "")
    {
        _prefix = prefix ?? string.Empty;
        _suffix = suffix ?? string.Empty;
    }

    private string GetStorageKey(string key, bool useRawKey) => useRawKey ? key : string.Concat(_prefix, key, _suffix);

    public bool Open(string dbFilePath)
    {
        if (string.IsNullOrEmpty(dbFilePath))
            return false;

        try
        {
            Connection ??= new SQLiteConnection(dbFilePath);
            _ = Connection.CreateTable<SimpleSetting>();

            return true;
        }
        catch (Exception)
        {
            return true;
        }
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

    //Left for compatability
    public void SetValue(string key, string? value)
    {
        var storageKey = GetStorageKey(key, true);

        SimpleSetting set = new()
        {
            Key = storageKey,
            Value = value
        };
        _ = InsertOrReplace(set);

        OnPropertyChanged(key);
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

    public bool ExistsSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        return Connection?.Table<SimpleSetting>().Where(v => v.Key == storageKey).Count() > 0;
    }

    private int? InsertOrReplace(SimpleSetting setting) => Connection?.InsertOrReplace(setting);

    public SimpleSetting? SelectSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        return Connection?.Table<SimpleSetting>().Where(v => v.Key == storageKey).FirstOrDefault();
    }

    public int? DeleteSetting(string key, bool useRawKey = false)
    {
        var storageKey = GetStorageKey(key, useRawKey);
        var ret = Connection?.Table<SimpleSetting>().Delete(v => v.Key == storageKey);
        OnPropertyChanged(key);
        return ret;
    }

    public List<SimpleSetting>? SelectAllSettings() => Connection?.CreateCommand("select * from SimpleSetting").ExecuteQuery<SimpleSetting>();

    public void Close() => Connection?.Dispose();

    public void Dispose()
    {
        Connection?.Close();
        Connection?.Dispose();

        GC.SuppressFinalize(this);
    }
}


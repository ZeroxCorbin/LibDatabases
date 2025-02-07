using CommunityToolkit.Mvvm.ComponentModel;
using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;

namespace LibSimpleDatabase;


public class SimpleDatabase : ObservableObject
{
    public class SimpleSetting
    {
        [PrimaryKey]
        public string? Key { get; set; } = string.Empty;
        public string? Value { get; set; } = string.Empty;
    }

    private SQLiteConnection? Connection { get; set; } = null;

    public SimpleDatabase? Open(string dbFilePath)
    {
        if (string.IsNullOrEmpty(dbFilePath))
            return null;

        try
        {
            Connection ??= new SQLiteConnection(dbFilePath);
            _ = Connection.CreateTable<SimpleSetting>();

            return this;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public T? GetValue<T>(string key, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false)
    {
        SimpleSetting? settings = SelectSetting(key);
        if (settings == null)
        {
            if (setDefault)
                SetValue<T>(key, default);

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
    public T? GetValue<T>(string key, T? defaultValue, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false)
    {
        SimpleSetting? settings = SelectSetting(key);
        if (settings == null)
        {
            if (setDefault)
                SetValue<T>(key, defaultValue);

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

        List<SimpleSetting>? settings = SelectAllSettings();
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

    public void SetValue(string key, string? value)
    {
        SimpleSetting set = new()
        {
            Key = key,
            Value = value
        };
        _ = InsertOrReplace(set);

        OnPropertyChanged(key);
    }
    public void SetValue<T>(string key, T? value, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false)
    {
        if (typeof(T) == typeof(string) || typeof(T).IsPrimitive)
        {
            SimpleSetting set = new()
            {
                Key = key,
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
                Key = key,
                Value = JsonConvert.SerializeObject(value, ser)
            };
            _ = InsertOrReplace(set);
        }

        OnPropertyChanged(key);
    }

    public bool ExistsSetting(string key) => Connection?.Table<SimpleSetting>().Where(v => v.Key == key).Count() > 0;

    private int? InsertOrReplace(SimpleSetting setting) => Connection?.InsertOrReplace(setting);
    public SimpleSetting? SelectSetting(string key) => Connection?.Table<SimpleSetting>().Where(v => v.Key == key).FirstOrDefault();
    public int? DeleteSetting(string key) { int? ret = Connection?.Table<SimpleSetting>().Delete(v => v.Key == key); OnPropertyChanged(key); return ret; }
    public List<SimpleSetting>? SelectAllSettings() => Connection?.CreateCommand("select * from SimpleSetting").ExecuteQuery<SimpleSetting>();

    public void Close() => Connection?.Dispose();
    public void Dispose()
    {
        Connection?.Close();
        Connection?.Dispose();

        GC.SuppressFinalize(this);
    }
}


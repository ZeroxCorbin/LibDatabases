using CommunityToolkit.Mvvm.ComponentModel;
using Newtonsoft.Json;
using SQLite;
using System;
using System.Collections.Generic;
using System.Text;

namespace LibSimpleDatabase
{

    public class SimpleDatabase : ObservableObject, IDisposable
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        public class SimpleSetting
        {
            [PrimaryKey]
            public string Key { get; set; } = string.Empty;
            public string Value { get; set; } = string.Empty;
        }

        private SQLiteConnection Connection { get; set; } = null;

        public SimpleDatabase Open(string dbFilePath)
        {
            Logger.Info("Opening Database: {file}", dbFilePath);

            if (string.IsNullOrEmpty(dbFilePath))
                return null;

            try
            {
                if (Connection == null)
                    Connection = new SQLiteConnection(dbFilePath);

                Connection.CreateTable<SimpleSetting>();

                return this;
            }
            catch (Exception e)
            {
                Logger.Error(e);
                return null;
            }
        }

        public string GetValue(string key, string defaultValue = "", bool setDefault = false)
        {
            SimpleSetting settings = SelectSetting(key);

            if (settings == null)
            {
                if (setDefault)
                    SetValue(key, defaultValue);

                return defaultValue;
            }
            else return settings.Value;
        }
        public T GetValue<T>(string key, bool setDefault = false)
        {
            SimpleSetting settings = SelectSetting(key);
            if (settings == null)
            {
                if (setDefault)
                    SetValue<T>(key, default(T));

                return default(T);
            }
            else return (T)Newtonsoft.Json.JsonConvert.DeserializeObject(settings.Value, typeof(T));


            //return settings == null ? default : (T)Newtonsoft.Json.JsonConvert.DeserializeObject(settings.Value, typeof(T));
        }
        public T GetValue<T>(string key, T defaultValue, bool setDefault = false)
        {
            SimpleSetting settings = SelectSetting(key);
            if (settings == null)
            {
                if (setDefault)
                    SetValue<T>(key, defaultValue);

                return defaultValue;
            }
            else return (T)Newtonsoft.Json.JsonConvert.DeserializeObject(settings.Value, typeof(T));
        }

        public List<T> GetAllValues<T>()
        {
            List<SimpleSetting> settings = SelectAllSettings();

            List<T> lst = new List<T>();

            foreach (SimpleSetting ss in settings)
            {
                lst.Add(ss.Value == string.Empty ? default : (T)Newtonsoft.Json.JsonConvert.DeserializeObject(ss.Value, typeof(T)));
            }
            return lst;
        }

        public void SetValue(string key, string value)
        {
            SimpleSetting set = new SimpleSetting()
            {
                Key = key,
                Value = value
            };
            _ = InsertOrReplace(set);

            OnPropertyChanged(key);
        }
        public void SetValue<T>(string key, T value)
        {
            SimpleSetting set = new SimpleSetting()
            {
                Key = key,
                Value = Newtonsoft.Json.JsonConvert.SerializeObject(value, new JsonSerializerSettings
                {
                    Error = (sender, errorArgs) => errorArgs.ErrorContext.Handled = true,
                    TypeNameHandling = TypeNameHandling.All
                })
            };

            _ = InsertOrReplace(set);

            OnPropertyChanged(key);
        }

        public bool ExistsSetting(string key) => Connection.Table<SimpleSetting>().Where(v => v.Key == key).Count() > 0;

        private int InsertOrReplace(SimpleSetting setting) => Connection.InsertOrReplace(setting);
        public SimpleSetting SelectSetting(string key) => Connection.Table<SimpleSetting>().Where(v => v.Key == key).FirstOrDefault();
        public int DeleteSetting(string key) { var ret = Connection.Table<SimpleSetting>().Delete(v => v.Key == key); OnPropertyChanged(key); return ret; }
        public List<SimpleSetting> SelectAllSettings() => Connection.CreateCommand("select * from SimpleSetting").ExecuteQuery<SimpleSetting>();

        public void Close() => Connection?.Dispose();
        public void Dispose()
        {
            Connection?.Close();
            Connection?.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}


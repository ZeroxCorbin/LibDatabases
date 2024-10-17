using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace LibSimpleDatabase
{
    public interface ISimpleDatabase : IDisposable
    {
        ISimpleDatabase? Open(string dbFilePath);
        T GetValue<T>(string key, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false);
        T GetValue<T>(string key, T defaultValue, bool setDefault = false, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false);
        List<T> GetAllValues<T>(TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false);
        void SetValue(string key, string value);
        void SetValue<T>(string key, T value, TypeNameHandling handling = TypeNameHandling.None, bool noErrors = false);
        bool ExistsSetting(string key);
        int? DeleteSetting(string key);
        void Close();
    }
}

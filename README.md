# Uma Musume: Load SQL Data [![.NET](https://github.com/SimpleSandman/UmaMusumeLoadSqlData/actions/workflows/dotnet.yml/badge.svg)](https://github.com/SimpleSandman/UmaMusumeLoadSqlData/actions/workflows/dotnet.yml)

This console app will take your DMM's `master.mdb` file and load it into either a MySQL/MariaDB or SQL Server database.

# Initial Setup

Add an `App.config` at the project level (same location as `.csproj`) and replace the following connection strings with yours. Also, set the config's "Copy to Output Directory" to `Copy if newer`.

If you don't have a MySQL/MariaDB or SQL Server database, you can leave the connection string empty and the program will skip it.

```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
  <connectionStrings>
    <add name="MySqlConnectionDev" connectionString="User Id=;Password=;Host=;Character Set=utf8mb4;AllowLoadLocalInfile=true" />
    <add name="MySqlConnectionProd" connectionString="User Id=;Password=;Host=;Character Set=utf8mb4;AllowLoadLocalInfile=true" />
    <add name="SqlServerConnectionDev" connectionString="Server=.;Database=UmaMusume;Integrated Security=SSPI;" />
    <add name="SqlServerConnectionProd" connectionString="Server=.;Database=UmaMusume;Integrated Security=SSPI;" />
  </connectionStrings>
</configuration>
```

This is using pre-processing with `#IF DEBUG` and it will select the connection strings based on if the solution is set to `Debug` or `Release`.

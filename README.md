# Uma Musume: Load SQL Data [![Build status](https://ci.appveyor.com/api/projects/status/19skk0jwbcy4ogy7/branch/master?svg=true&passingText=deployment%20-%20OK&failingText=deployment%20-%20FAILED)](https://ci.appveyor.com/project/SimpleSandman/umamusumeloadsqldata/branch/master)

This console app will take your DMM's `master.mdb` file and load it into either a MySQL/MariaDB or SQL Server database.

# Command Line Arguments

- Environment *(required)*
  - Set to "Development" or "Production"
- Owner/Repo *(required)*
  - Path to the `master.mdb` on GitHub
- Repo Branch *(required)*
  - Download `master.mdb` from specified GitHub branch
- MySQL Connection String *(required)*
  - Set to "N/A" to skip
- SQL Server Connection String *(required)*
  - Set to "N/A" to skip

### Example:
```cmd
dotnet run "Development" "SimpleSandman/UmaMusumeMasterMDB" "master" "user id=;password=;host=;database=;character set=utf8mb4;AllowLoadLocalInfile=true" "Server=;Database=;Integrated Security=SSPI;"
```

# Uma Musume: Load SQL Data [![Build status](https://ci.appveyor.com/api/projects/status/19skk0jwbcy4ogy7/branch/master?svg=true&passingText=deployment%20-%20OK&failingText=deployment%20-%20FAILED)](https://ci.appveyor.com/project/SimpleSandman/umamusumeloadsqldata/branch/master)

This console app will take your DMM's `master.mdb` file and load it into a MySQL/MariaDB.

## Retired Repository

This project was retired on October 29, 2025, and the hosted API and its supporting apps have been shut down. I worked with Katboi from UmaViewer beforehand, so that app wasn't affected.

I had been meaning to step away from maintaining this project. By the time I retired it, I hadn't played the game in over three years, but I've always enjoyed this community and wanted to provide some kind of support for as long as I could. I open-sourced everything except the CI/CD, which was a very simple setup, so anyone who wants to pick up where I left off is welcome to fork it.

# Command Line Arguments

- Environment *(required)*
  - Set to "Development" or "Production"
- Owner/Repo *(required)*
  - Path to the `master.mdb` on GitHub
- Repo Branch *(required)*
  - Download `master.mdb` from specified GitHub branch
- MySQL Connection String *(required)*
  - Set to "N/A" to skip

### Example:
```cmd
dotnet run "Development" "SimpleSandman/UmaMusumeMasterMDB" "master" "user id=;password=;host=;database=;character set=utf8mb4;AllowLoadLocalInfile=true"
```

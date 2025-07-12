@echo off
echo Building KB Memory .NET Solution...
dotnet restore
dotnet build
echo Build completed.
pause

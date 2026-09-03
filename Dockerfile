FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore avval — Docker cache dan foydalanish uchun
COPY TbMigrator.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish TbMigrator.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
COPY config.yaml .
ENTRYPOINT ["dotnet", "tbmigrator.dll"]

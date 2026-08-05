FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["OrderProcessing.sln", "."]
COPY ["src/Shared/Shared.csproj", "src/Shared/"]
COPY ["src/OrderService/OrderService.csproj", "src/OrderService/"]
COPY ["src/PaymentService/PaymentService.csproj", "src/PaymentService/"]
COPY ["src/NotificationService/NotificationService.csproj", "src/NotificationService/"]

RUN dotnet restore "src/NotificationService/NotificationService.csproj"

COPY . .
WORKDIR "/src/src/NotificationService"
RUN dotnet publish "NotificationService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER app
ENTRYPOINT ["dotnet", "NotificationService.dll"]

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["OrderProcessing.sln", "."]
COPY ["src/Shared/Shared.csproj", "src/Shared/"]
COPY ["src/OrderService/OrderService.csproj", "src/OrderService/"]
COPY ["src/PaymentService/PaymentService.csproj", "src/PaymentService/"]
COPY ["src/NotificationService/NotificationService.csproj", "src/NotificationService/"]

RUN dotnet restore "src/OrderService/OrderService.csproj"

COPY . .
WORKDIR "/src/src/OrderService"
RUN dotnet publish "OrderService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER app
EXPOSE 5000
ENV ASPNETCORE_URLS=http://+:5000
ENTRYPOINT ["dotnet", "OrderService.dll"]

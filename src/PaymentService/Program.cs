using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PaymentService;
using Shared.Db;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("OrdersDb")
    ?? "Host=localhost;Port=5432;Database=ordersdb;Username=orders;Password=orders";

builder.Services.AddSingleton<IDbConnectionFactory>(new DbConnectionFactory(connectionString));
builder.Services.AddHostedService<PaymentWorker>();

var host = builder.Build();
host.Run();

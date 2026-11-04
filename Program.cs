using System.Diagnostics;
using System.Security.Claims;
using System.Text.Json;
using BeanNotifs.HealthChecks;
using BeanNotifs.Messaging;
using BeanNotifs.Models;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using OpenTelemetry.Trace;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// otel-tracing plan Step 3/Q4: surfaces the active trace/span ID in log scopes
// (Console:IncludeScopes in appsettings.json renders them) so a trace can be
// cross-referenced against kubectl logs / Loki without a UI click-through.
builder.Logging.Configure(options =>
{
    options.ActivityTrackingOptions = ActivityTrackingOptions.TraceId
        | ActivityTrackingOptions.SpanId
        | ActivityTrackingOptions.ParentId;
});

// otel-tracing plan Step 3: AddSource("BeanNotifs") picks up the manual
// consume-side span (EventConsumerHostedService.cs, linked to Pay's/Cooker's
// publish spans via the propagated traceparent header) -- Notifs has no
// EventPublisher, it's the end of the chain. OTEL_SERVICE_NAME/
// OTEL_EXPORTER_OTLP_ENDPOINT are set as env vars in deploy/deployment.yaml,
// read automatically by AddOtlpExporter.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource("BeanNotifs")
        .AddOtlpExporter());

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection(RabbitMqOptions.SectionName));
builder.Services.AddSingleton<NotificationStore>();

// SSO plan Step 3: /notifications carries admin-only data (the Admin page's own
// log of orders/payments). BeanApp forwards the logged-in admin's own Keycloak
// token, so this validates it the same way OrderBeans does (issuer, signature,
// shared audience from Step 1) plus the "admin" realm role, which isn't a claim
// ASP.NET Core maps to ClaimTypes.Role on its own.
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        var authority = builder.Configuration["Keycloak:Authority"];
        var publicAuthority = builder.Configuration["Keycloak:PublicAuthority"];

        options.Authority = authority;
        options.RequireHttpsMetadata = false; // Keycloak runs on plain HTTP inside the cluster
        options.TokenValidationParameters.ValidateAudience = true;
        options.TokenValidationParameters.ValidAudience = "phaseolus-internal-api";
        options.MapInboundClaims = false;

        // The token this endpoint actually receives is the admin's own token from
        // BeanApp's browser login, which Keycloak stamps with whatever host the
        // browser used (PublicAuthority) -- not the in-cluster DNS name (Authority)
        // this backend uses to reach Keycloak for discovery. Same mismatch BeanApp's
        // own login hit; same fix.
        if (!string.IsNullOrEmpty(publicAuthority))
        {
            options.TokenValidationParameters.ValidIssuer = publicAuthority;
        }

        options.Events = new JwtBearerEvents
        {
            OnTokenValidated = ctx =>
            {
                if (ctx.Principal?.Identity is not ClaimsIdentity identity)
                {
                    return Task.CompletedTask;
                }

                var realmAccess = identity.FindFirst("realm_access")?.Value;
                if (realmAccess is null)
                {
                    return Task.CompletedTask;
                }

                using var doc = JsonDocument.Parse(realmAccess);
                if (doc.RootElement.TryGetProperty("roles", out var roles))
                {
                    foreach (var role in roles.EnumerateArray())
                    {
                        identity.AddClaim(new Claim(ClaimTypes.Role, role.GetString()!));
                    }
                }

                return Task.CompletedTask;
            },
        };
    });
builder.Services.AddAuthorization();

var notificationsSent = Metrics.CreateCounter("notifications_sent_total", "Total number of notifications sent.");

builder.Services.AddSingleton<IReadOnlyList<ConsumerBinding>>(sp =>
{
    var store = sp.GetRequiredService<NotificationStore>();
    var logger = sp.GetRequiredService<ILogger<Program>>();
    return new List<ConsumerBinding>
    {
        new("notifs.payment-succeeded", "payment.succeeded", envelope => HandlePaymentSucceededAsync(envelope, store, notificationsSent, logger)),
        new("notifs.payment-failed", "payment.failed", envelope => HandlePaymentFailedAsync(envelope, store, notificationsSent, logger)),
        new("notifs.order-cooking-started", "order.cooking-started", envelope => HandleOrderCookingStartedAsync(envelope, store, notificationsSent, logger)),
        new("notifs.order-cooked", "order.cooked", envelope => HandleOrderCookedAsync(envelope, store, notificationsSent, logger)),
    };
});

// Registered both as its own type (so the health check and DI can resolve it directly)
// and as the IHostedService that actually gets started — same singleton instance either way.
builder.Services.AddSingleton<EventConsumerHostedService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<EventConsumerHostedService>());

builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: ["live"])
    .AddCheck<RabbitMqHealthCheck>("rabbitmq", tags: ["ready"]);

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

app.UseHttpMetrics();
app.MapMetrics();

app.MapHealthChecks("/healthz/live", new() { Predicate = c => c.Tags.Contains("live") });
app.MapHealthChecks("/healthz/ready", new() { Predicate = c => c.Tags.Contains("ready") });

app.MapGet("/notifications", (NotificationStore store) => store.GetAll())
    .RequireAuthorization(policy => policy.RequireRole("admin"));

app.Run();

static Task HandlePaymentSucceededAsync(EventEnvelope envelope, NotificationStore store, Counter notificationsSent, ILogger<Program> logger)
{
    store.Add(new Notification(envelope.OrderId, "payment-succeeded", "Payment succeeded.", envelope.OccurredAt));
    notificationsSent.Inc();
    logger.LogInformation("Notification logged for order {OrderId}: payment-succeeded", envelope.OrderId);
    return Task.CompletedTask;
}

static Task HandlePaymentFailedAsync(EventEnvelope envelope, NotificationStore store, Counter notificationsSent, ILogger<Program> logger)
{
    var payload = envelope.Payload.Deserialize<PaymentFailedPayload>()!;
    store.Add(new Notification(envelope.OrderId, "payment-failed", $"Payment failed: {payload.Reason}", envelope.OccurredAt));
    notificationsSent.Inc();
    logger.LogInformation("Notification logged for order {OrderId}: payment-failed", envelope.OrderId);
    return Task.CompletedTask;
}

static Task HandleOrderCookingStartedAsync(EventEnvelope envelope, NotificationStore store, Counter notificationsSent, ILogger<Program> logger)
{
    store.Add(new Notification(envelope.OrderId, "order-cooking-started", "Your order has been accepted by the kitchen.", envelope.OccurredAt));
    notificationsSent.Inc();
    logger.LogInformation("Notification logged for order {OrderId}: order-cooking-started", envelope.OrderId);
    return Task.CompletedTask;
}

static Task HandleOrderCookedAsync(EventEnvelope envelope, NotificationStore store, Counter notificationsSent, ILogger<Program> logger)
{
    store.Add(new Notification(envelope.OrderId, "order-cooked", "Your order is ready!", envelope.OccurredAt));
    notificationsSent.Inc();
    logger.LogInformation("Notification logged for order {OrderId}: order-cooked", envelope.OrderId);
    return Task.CompletedTask;
}

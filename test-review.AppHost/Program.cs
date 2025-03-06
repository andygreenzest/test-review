var builder = DistributedApplication.CreateBuilder(args);

var apiService = builder.AddProject<Projects.test_review_ApiService>("apiservice");

builder.AddProject<Projects.test_review_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithReference(apiService)
    .WaitFor(apiService);

builder.Build().Run();

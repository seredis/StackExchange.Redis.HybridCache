// NuGet package health check, equivalent to the nuget.org / NuGet Package Explorer
// green check marks: every lib/ assembly must have an embedded portable PDB, a
// SourceLink map pointing at this repository, and the Reproducible (deterministic)
// debug-directory entry. Also asserts the package-level metadata that nuget.org
// surfaces (license, readme, icon, repository commit).
//
// Usage: dotnet run eng/packcheck.cs <path-to-nupkg>

// no AOT: the default file-based-app ILCompiler reference trips NU1903 under the
// repo's TreatWarningsAsErrors, and nothing here needs publishing
#:property PublishAot=false

using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Xml.Linq;

if (args.Length != 1 || !File.Exists(args[0]))
{
    Console.Error.WriteLine("usage: dotnet run eng/packcheck.cs <path-to-nupkg>");
    return 2;
}

int failures = 0;
void Check(bool ok, string what, string? detail = null)
{
    Console.WriteLine($"{(ok ? "  ok" : "FAIL")}: {what}{(detail is null ? "" : $" ({detail})")}");
    if (!ok) failures++;
}

using var zip = ZipFile.OpenRead(args[0]);

// package-level metadata
var nuspecEntry = zip.Entries.Single(e => e.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase) && !e.FullName.Contains('/'));
XDocument nuspec;
using (var stream = nuspecEntry.Open())
{
    nuspec = XDocument.Load(stream);
}

XNamespace ns = nuspec.Root!.GetDefaultNamespace();
var metadata = nuspec.Root.Element(ns + "metadata")!;
string? Meta(string name) => metadata.Element(ns + name)?.Value;

Check(Meta("license") == "MIT", "license expression", Meta("license"));
string? readme = Meta("readme");
Check(readme is not null && zip.GetEntry(readme) is not null, "readme packed", readme);
string? icon = Meta("icon");
Check(icon is not null && zip.GetEntry(icon) is not null, "icon packed", icon);
var repository = metadata.Element(ns + "repository");
string? commit = repository?.Attribute("commit")?.Value;
Check(commit is { Length: 40 }, "repository commit stamped", commit);
string? repoUrl = repository?.Attribute("url")?.Value;
Check(repoUrl is not null, "repository url", repoUrl);

// per-assembly checks
var assemblies = zip.Entries.Where(e =>
    e.FullName.StartsWith("lib/", StringComparison.OrdinalIgnoreCase) &&
    e.FullName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)).ToList();
Check(assemblies.Count > 0, "package contains lib/ assemblies", assemblies.Count.ToString());

foreach (var entry in assemblies)
{
    Console.WriteLine(entry.FullName);
    using var raw = entry.Open();
    using var buffer = new MemoryStream();
    raw.CopyTo(buffer);
    buffer.Position = 0;
    using var pe = new PEReader(buffer);

    var debugEntries = pe.ReadDebugDirectory();
    Check(debugEntries.Any(d => d.Type == DebugDirectoryEntryType.Reproducible), "deterministic (Reproducible entry)");

    var embedded = debugEntries.Where(d => d.Type == DebugDirectoryEntryType.EmbeddedPortablePdb).ToList();
    Check(embedded.Count == 1, "embedded portable PDB");
    if (embedded.Count != 1)
    {
        continue;
    }

    using var pdb = pe.ReadEmbeddedPortablePdbDebugDirectoryData(embedded[0]);
    var reader = pdb.GetMetadataReader();

    // SourceLink lives in a CustomDebugInformation blob on the module
    Guid sourceLinkKind = new("CC110556-A091-4D38-9FEC-25AB9A351A6A");
    string? sourceLink = null;
    foreach (var handle in reader.CustomDebugInformation)
    {
        var cdi = reader.GetCustomDebugInformation(handle);
        if (reader.GetGuid(cdi.Kind) == sourceLinkKind)
        {
            sourceLink = Encoding.UTF8.GetString(reader.GetBlobBytes(cdi.Value));
            break;
        }
    }

    Check(sourceLink is not null, "SourceLink present");
    if (sourceLink is not null && commit is not null)
    {
        Check(sourceLink.Contains("seredis/StackExchange.Redis.HybridCache") && sourceLink.Contains(commit),
            "SourceLink targets this repository at the packed commit");
    }

    // every source document should map through the SourceRoot (i.e. be available
    // via SourceLink or be embedded); spot the compiler's /_/-style mapped paths
    int unmapped = 0;
    foreach (var docHandle in reader.Documents)
    {
        var doc = reader.GetDocument(docHandle);
        string name = reader.GetString(doc.Name);
        if (!name.StartsWith("/_", StringComparison.Ordinal))
        {
            unmapped++;
        }
    }

    Check(unmapped == 0, "all source paths are deterministic-mapped", unmapped == 0 ? null : $"{unmapped} unmapped");
}

Console.WriteLine(failures == 0 ? "PACKAGE OK" : $"{failures} CHECK(S) FAILED");
return failures == 0 ? 0 : 1;

# Maintainer Guide

Internal documentation for `Akka.Surgewave` maintainers. The repository ships two NuGet packages from one tag.

## Release Process

### 1. Prepare

```bash
dotnet test Akka.Surgewave.slnx -c Release -v normal
```

Akka-TCK-Specs (Journal/SnapshotStore/ReadJournal) and EndToEnd tests need a running Surgewave broker. The release workflow filters them via `FullyQualifiedName!~Spec&FullyQualifiedName!~EndToEnd`; a dedicated integration job with Testcontainers exercises them on `main`.

### 2. Bump versions

`Directory.Build.props` carries one `<Version>` for both projects; bumping there releases both NuGets at the same number. There is intentionally no per-package version drift.

### 3. Tag and Push

```bash
# Stable release
git tag v0.2.0
git push --tags

# Pre-release
git tag v0.3.0-rc.1
git push --tags
```

### 4. What happens automatically

- `.github/workflows/release.yml` triggers on `v*` tag push
- Build + Test + Pack run against the tag version (slnx covers both projects)
- Two `*.nupkg` artifacts:
  - `Kuestenlogik.Surgewave.AkkaStreams.<v>.nupkg`
  - `Kuestenlogik.Surgewave.AkkaPersistence.<v>.nupkg`
- Both push to GitHub Packages (stable + pre-release)
- Both push to nuget.org (stable only, gated on `NUGET_API_KEY` secret)
- GitHub Release with auto-generated notes + both nupkgs attached

## Tag naming

- `v{major}.{minor}.{patch}` — stable
- `v{major}.{minor}.{patch}-rc.{n}` — release candidate (skipped on nuget.org push)

## Secret requirements

| Secret | Scope | Used for |
|---|---|---|
| `NUGET_API_KEY` | Org-level | nuget.org publish (gate on `env.X != ''`). Glob should include `Kuestenlogik.Surgewave.Akka*` to cover both packages. |
| `KUESTENLOGIK_PACKAGES_TOKEN` | Org-level | Restore from GitHub Packages during build (Surgewave-Client dependency) |

If `NUGET_API_KEY` is missing, the workflow skips nuget.org silently and GitHub Packages still receives the build.

## NuGet package naming

Aligned across all artifacts for both packages:

| Property | AkkaStreams | AkkaPersistence |
|---|---|---|
| **Repo** | `Akka.Surgewave` (shared) | `Akka.Surgewave` (shared) |
| **csproj-Folder** | `src/Kuestenlogik.Surgewave.AkkaStreams/` | `src/Kuestenlogik.Surgewave.AkkaPersistence/` |
| **csproj name** | `Kuestenlogik.Surgewave.AkkaStreams.csproj` | `Kuestenlogik.Surgewave.AkkaPersistence.csproj` |
| **Assembly name** | `Kuestenlogik.Surgewave.AkkaStreams` | `Kuestenlogik.Surgewave.AkkaPersistence` |
| **C# Namespace** | `Kuestenlogik.Surgewave.AkkaStreams` | `Kuestenlogik.Surgewave.AkkaPersistence` |
| **NuGet PackageId** | `Kuestenlogik.Surgewave.AkkaStreams` | `Kuestenlogik.Surgewave.AkkaPersistence` |

`AkkaStreams` / `AkkaPersistence` are deliberately single-token (no `Akka.Streams` / `Akka.Persistence` sub-namespace), so the C# compiler does not confuse `using Akka.Streams.Dsl;` (the external Akka.NET package) with our own namespace tree. The `Akka.*` prefix on nuget.org is verified-reserved by the Akka.NET team (owner `Akka`); a direct push under `Akka.Streams.Surgewave` / `Akka.Persistence.Surgewave` returns `409 Conflict`.

## Predecessor repos

`Akka.Streams.Surgewave` (v0.1.0–v0.1.1) and `Akka.Persistence.Surgewave` (v0.1.0–v0.1.1) shipped from separate repositories with PackageIds `Kuestenlogik.Akka.Streams.Surgewave` and `Kuestenlogik.Akka.Persistence.Surgewave`. Those packages remain on nuget.org for existing consumers; they are no longer updated. v0.2.0+ ships only the new ids from this combined repo.

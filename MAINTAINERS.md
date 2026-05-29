# Maintainer Guide

Internal documentation for `Akka.Surgewave` maintainers. The repository ships two NuGet packages from one tag.

## Release Process

### 1. Prepare

```bash
dotnet test Akka.Surgewave.slnx -c Release -v normal
```

The Akka-TCK specs (Journal-/SnapshotStore-TCK) and EndToEnd tests run against a real Surgewave broker started **in-process** by `SurgewaveBrokerFixture` (an `ICollectionFixture` wrapping `SurgewaveRuntime` — in-memory storage, auto-assigned port, auto-created topics). No external broker / Docker is needed. `main`-CI (`ci.yml`) runs the full suite including these; the release workflow (`release.yml`) still filters them via `FullyQualifiedName!~Spec&FullyQualifiedName!~EndToEnd` to keep the tag path lean (the specs already gated the commit on `main`).

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
  - `Kuestenlogik.Akka.Surgewave.Streams.<v>.nupkg`
  - `Kuestenlogik.Akka.Surgewave.Persistence.<v>.nupkg`
- Both push to GitHub Packages (stable + pre-release)
- Both push to nuget.org (stable only, gated on `NUGET_API_KEY` secret)
- GitHub Release with auto-generated notes + both nupkgs attached

## Tag naming

- `v{major}.{minor}.{patch}` — stable
- `v{major}.{minor}.{patch}-rc.{n}` — release candidate (skipped on nuget.org push)

## Secret requirements

| Secret | Scope | Used for |
|---|---|---|
| `NUGET_API_KEY` | Org-level | nuget.org publish (gate on `env.X != ''`). Glob should include `Kuestenlogik.Akka.Surgewave.*` to cover both packages. |
| `KUESTENLOGIK_PACKAGES_TOKEN` | Org-level | Restore from GitHub Packages during build (Surgewave-Client dependency) |

If `NUGET_API_KEY` is missing, the workflow skips nuget.org silently and GitHub Packages still receives the build.

## NuGet package naming

Aligned across all artifacts for both packages:

| Property | Streams | Persistence |
|---|---|---|
| **Repo** | `Akka.Surgewave` (shared) | `Akka.Surgewave` (shared) |
| **csproj-Folder** | `src/Kuestenlogik.Akka.Surgewave.Streams/` | `src/Kuestenlogik.Akka.Surgewave.Persistence/` |
| **csproj name** | `Kuestenlogik.Akka.Surgewave.Streams.csproj` | `Kuestenlogik.Akka.Surgewave.Persistence.csproj` |
| **Assembly name** | `Kuestenlogik.Akka.Surgewave.Streams` | `Kuestenlogik.Akka.Surgewave.Persistence` |
| **C# Namespace** | `Kuestenlogik.Akka.Surgewave.Streams` | `Kuestenlogik.Akka.Surgewave.Persistence` |
| **NuGet PackageId** | `Kuestenlogik.Akka.Surgewave.Streams` | `Kuestenlogik.Akka.Surgewave.Persistence` |

The `.Surgewave.` segment between `Akka` and `Streams`/`Persistence` is deliberate: the namespace never contains the substring `Akka.Streams` or `Akka.Persistence`, so the C# compiler does not confuse `using Akka.Streams.Dsl;` (the external Akka.NET package) with our own namespace tree. The `Akka.*` prefix on nuget.org is verified-reserved by the Akka.NET team (owner `Akka`); a direct push under `Akka.Streams.Surgewave` / `Akka.Persistence.Surgewave` returns `409 Conflict`.

A short-lived v0.2.0 shipped under the interim ids `Kuestenlogik.Surgewave.AkkaStreams` / `.AkkaPersistence`; v0.3.0 onwards uses `Kuestenlogik.Akka.Surgewave.{Streams,Persistence}` and the v0.2.0 ids are unlisted.

## Predecessor repos

`Akka.Streams.Surgewave` (v0.1.0–v0.1.1) and `Akka.Persistence.Surgewave` (v0.1.0–v0.1.1) shipped from separate repositories with PackageIds `Kuestenlogik.Akka.Streams.Surgewave` and `Kuestenlogik.Akka.Persistence.Surgewave`. Those packages remain on nuget.org for existing consumers; they are no longer updated. v0.2.0+ ships only the new ids from this combined repo.

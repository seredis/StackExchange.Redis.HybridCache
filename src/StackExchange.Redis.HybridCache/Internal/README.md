# Internal design

This folder will hold the machinery ported from `Microsoft.Extensions.Caching.Hybrid`
(dotnet/extensions, MIT — see `THIRD-PARTY-NOTICES.md` at the repo root) plus the
redis-native backend. The fork map — what is ported intact (stampede state machine,
cache-item/buffer lifetime, serialization pipeline, payload envelope), what is dropped
(`IDistributedCache` L2, `MemoryCache` L1), and what is new (plain-string redis read/write
path with absolute expiry, client-side-cached `__SER_HCT__` tag timestamps) — is in
[`docs/design.md`](../../../docs/design.md).

Ported files keep their original .NET Foundation copyright header with a provenance line.

# Reviewed IARI source evidence

`reviewed-2026.zip` is a 22.8 MB frozen public evidence bundle for the scheduler
and its offline replay. It contains 61 WFS responses, the 15 November summary PDF,
four exceptional daily PDFs and `bundle.json` with public URLs and SHA-256 hashes.
The source is https://creams.iari.res.in/ (CREAMS, ICAR-IARI). No personal data,
credentials or local machine paths are included. The archive is never placed in
the public Pages artifact; only committed `docs` is deployed.

The bundle covers 15 September–15 November 2026. 57 days use valid WFS records;
four use individually reviewed bulletin event tables; one is bulletin-confirmed
zero without a WFS layer. The importer revalidates every source and all 124 daily
and 46 district totals on every build. `policy.json` pins the archive bytes.

`expected-20261115.json` pins all 126 recovered output file hashes using the
published baseline already frozen in `tests/fixtures/recovery/inputs.zip`.
The scheduled replay verifies these exact outputs before testing new ingestion.

Do not refresh this archive automatically. A changed prefix requires review of
source corrections, a new archive digest and updated independently verified replay
outputs. Subsequent daily defects also require review; the scheduler cannot invent
zeros, silently use a different satellite feed or apply unreviewed PDF corrections.

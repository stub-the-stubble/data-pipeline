Set up environment

```
conda env create -f environment.yml
conda activate env
```

Run script

```
python get_nrsc_data.py
```

Data files are generated and written into `docs/` folder

## Local verification (v2)

The scheduled pipeline uses the explicit Linux environment in `spec-file.txt`.
To test without writing published data:

```sh
python -m unittest discover -s tests -v
python v2/get_nrsc_data.py 2026-10-23 --temp
# Replay a downloaded CSV without another network request:
python v2/get_nrsc_data.py 2026-10-23 --temp --csv
```

`--temp` creates the output folders under `temp/`. Downloaded archives are retained
there for inspection. Omitting `--temp` writes into `docs/v2/`.

A complete, readable shapefile with the expected schema may contain zero records.
A missing, invalid or projection-only archive is an error, not a zero-fire day.
Both sensors must load and both states must pass validation before daily
outputs are written. Multiply-mapped fire locations stop the run. Unmapped
locations retain the source state and a null district, with
`district_assignment: "unassigned"`. They are included in state totals and
separately counted in `unassigned_total` (daily) and `unassigned.dates` (history).
The invariant is: named district counts + unassigned count = state total.
No nearest-neighbour assignment or boundary snapping is performed. Historical
absence of this metadata means unknown coverage, not zero unassigned fires.
The frontend must display this uncertainty alongside the district figures.

The scheduled job still processes only the current IST day. Restoring service
will not automatically backfill earlier missing days: each accepted date needs a
separate reviewed run. Current-day observations may be incomplete while the
source continues updating. A local test does not verify the hosted runner.

## Safe offline backfill

`v2/backfill.py` uses frozen CSV inputs. It never downloads, commits, pushes,
updates the baseline, or deploys anything. Run from the repository root.

```sh
# Default: calculate and validate in a temporary directory, then discard it.
python v2/backfill.py --baseline ../release/baseline --csv-dir temp/csv \
  --start 2026-10-23 --end 2026-11-15

# Explicitly save a candidate for review, outside the repository and input trees.
python v2/backfill.py --baseline ../release/baseline --csv-dir temp/csv \
  --start 2026-10-23 --end 2026-11-15 --build --work-dir ../release/candidate

# Continue a clean checkpoint; changed inputs/code/outputs are rejected.
python v2/backfill.py --baseline ../release/baseline --csv-dir temp/csv \
  --start 2026-10-23 --end 2026-11-15 --build --work-dir ../release/candidate --resume
```

The baseline must contain both complete published historical JSONs, copied from a
recorded production commit. For the eventual release, use a fresh full checkout's
`docs/v2` as the baseline so all existing files are fingerprinted. Existing dates
are refused; this is a missing-date recovery tool, not a historical rewrite tool.
Current-day inputs require `--include-current-day` and remain provisional.

The review directory contains `candidate/` and `manifest.json`. Only a manifest
with `status: complete` is eligible for review. It records the exact output
allowlist and hashes, input CSV/baseline/code/boundary hashes, dates, totals and
unassigned counts. Earlier observations are preserved. Copying or merging into
an actual release checkout is intentionally a separate reviewed step; there is
no apply/publish flag. Recheck the production commit and baseline hashes first.
Do not copy the entire `temp/` directory.

Normal failures retain a per-day checkpoint. If a process is forcibly killed,
a leftover `.lock` or mismatched output hashes cause a refusal, not automatic
repair. Preserve the evidence and build a fresh candidate after inspection.

## Pages deployment (prepared locally; settings not changed)

The workflow has three manual modes:
- `validate` (default): offline tests and published-data checks, no ingestion,
  commit, upload or deployment.
- `ingest-and-deploy`: retrieve the latest completed IARI bulletin, reconcile it, commit only state
  JSON files, then explicitly publish the committed snapshot.
- `deploy-existing`: validate and publish data already committed to main; no
  source downloads or data commit. Use after an approved backfill commit.

Every publishing path requires `main` and repository variable
`STUB_PUBLISH_ENABLED=true`. Hourly ingestion additionally requires
`STUB_SCHEDULE_ENABLED=true`. Leave the scheduling flag absent/false throughout
manual backfill publication; concurrency alone does not pause the hourly writer.
With the publication flag absent/false, scheduled writes are skipped and manual
publishing modes fail. Set the Pages source to GitHub
Actions and configure the `github-pages` environment for main (and any required
reviewer) during the approved release. These settings are not changed by the
local code preparation. Keep the variable disabled until the frontend, runtime,
backup and backfill review are ready.

All supported modes share `nrsc-data-release` concurrency. The artifact comes
from `git archive` of the committed `docs` tree, excluding untracked downloads.
Both preparation and deployment check the expected main commit. External manual
writers must still be held/coordinated during release; workflow concurrency
cannot lock a maintainer's separate git push. The deploy job uses scoped
`pages:write` and `id-token:write`; no personal token is needed.

A commit succeeding does not prove deployment succeeded. If deployment fails,
keep writes held, diagnose the failure, and use an approved `deploy-existing`
run for the reviewed commit. Verify the public JSON after deployment. Hosted
Conda execution and offline tests have passed hosted validation. Environment
approval rules and actual Pages delivery still require an approved release.

### Frozen recovery regression

`python tests/replay_recovery.py` replays 23 October–15 November 2026 from
`tests/fixtures/recovery/inputs.zip` into temporary storage. These are public
NRSC observations and the preserved published baseline, not credentials.
`expected.json` records input and semantic output hashes. Every output field
except generated `last_update` timestamps must match the reviewed local build.
The test reconciles all 48 daily files and both histories and checks resumability.
It performs no network downloads, repository writes, commits or deployments.

## IARI paddy importer (local candidate preparation)

The reviewed 2026 paddy series uses IARI CREAMS state-assigned event records,
validated against independently published daily and district bulletin totals.
It is not interchangeable with the older NRSC detections or a count of unique
physical fires. Cross-state observations in IARI's records are retained and marked.
Do not combine the old NRSC backfill candidate with this replacement season.

Install `requirements-iari.txt` in the pipeline environment. Download evidence
to a new directory outside the repository; the downloader never publishes:

```sh
python v2/iari_fetch.py --summary-url https://creams.iari.res.in/Creams_website_bulletin/Fire_bulletin_2026/Fire_bulletin_2026r/62.RiceResidueFireBulletin_15Nov_2026_ICAR.pdf --through 2026-11-15 --output ../evidence/iari-bundle
```

Review `validation-findings.json`. Missing data never becomes zero unless the
bulletin explicitly reports zero. Exceptional dates require a reviewed `fallback`
entry in the frozen bundle, with `kind: bulletin-events`, a reason,
`rejected_wfs_sha256`, and a `bulletin` descriptor (`path`, public IARI `url`,
`sha256`). Preserve the failed WFS bytes. Download the corresponding public PDF
and review its event table before adding the fallback. A missing layer uses a null
rejected hash. Optional row corrections must match state, serial, original place,
latitude and longitude exactly, and specify the district and reason. Never infer
district corrections from our display polygons.

The downloader also accepts `--reviewed-fallbacks` (a date-keyed JSON object with
the same rules, replacing `bulletin` with `bulletin_url`). A re-download can change
WFS timestamps/hashes; old approvals deliberately do not transfer to changed
bytes. Review new evidence rather than bypassing the hash check.

Build a new candidate offline, preserving history outside the complete season:

```sh
python v2/iari_import.py --bundle ../evidence/iari-bundle/bundle.json --baseline ../release/baseline/v2 --output ../release/iari-candidate
python -m unittest discover -s tests
```

The importer supports the checked 2026 paddy layout only, completed IST days,
and the full season starting 15 September. It checks source hashes, CRS,
coordinates, dates, satellites, row identities, full pagination, exact daily
counts, district totals and history consistency. Invalid input leaves no partial
candidate. Existing output directories cannot be overwritten. The manifest records
input, baseline, implementation and output hashes.

The 15 November proof needs four reviewed PDF event fallbacks: 20 October
(district/block swap), 21 October (20 missing Punjab WFS records), 2 and 3 November
(incorrect WFS dates). 17 September is bulletin-confirmed zero without a WFS
layer. Other 57 days use WFS. All 124 state/day counts and 46 district totals match.

The candidate is an **overlay**, containing 124 daily files, two histories and
an audit manifest. It is not a complete Pages archive: older daily files must be
retained from the recorded baseline. Do not place the audit manifest in the public
v2 API. Recheck the baseline commit and hashes before any approved application.

The scheduled workflow now uses `v2/iari_schedule.py`; NRSC is retained only for
legacy regression and historical investigation. Keep publication and scheduling
gates disabled until hosted replay, baseline backup, candidate application and
end-to-end deployment checks are approved. No credentials are needed for the
public IARI sources. See `geojson/iari-2026/README.md` for map provenance and limits.

## Scheduled IARI ingestion

The hourly trigger checks actual PDF links on IARI's bulletin index and chooses
the latest completed observation day in IST. It does not guess URLs or assume
today has been published. The PDF observation date must match the selected link.
The current policy supports the 2026 paddy season only. More than two completed
days of lag, a missing index, an ambiguous link or a failed download holds
publication. The seven days after 30 November allow the final bulletin to arrive;
after that, or in a new year, the policy must be reviewed before ingestion resumes.

`data/iari/reviewed-2026.zip` contains the frozen public evidence through
15 November, including all four reviewed PDF exceptions. Its SHA-256 is pinned
in `data/iari/policy.json`. Every run re-parses it, then downloads all subsequent
season days and reconciles the **entire season** against the latest bulletin's
daily and district tables. A new nonzero day without a valid WFS layer fails.
A missing layer is zero only when both states are explicitly zero in the bulletin.
There is no automatic PDF fallback or district correction for new defects.

The reviewed prefix is intentionally frozen. Later bulletin count/district
revisions that disagree with it block the run; silent coordinate-only revisions
to old live WFS layers are not polled. To accept a correction, review a new frozen
bundle, update its pinned archive/hash and expected replay outputs, and rerun
validation before release. Historical corrections are source changes, not retries.

Before scheduling, the separately reviewed IARI backfill must already be applied:
both states need contiguous IARI-labelled daily files and histories through the
reviewed prefix. The scheduler refuses to migrate an NRSC or mixed season itself.
It prepares a complete release tree outside the repository, retaining older files,
validates all daily/history consistency, and checks that the baseline did not move.

For a read-only source check and local preparation (no repository writes):

```sh
python v2/iari_schedule.py --baseline ../release/complete-v2 --work-dir ../release/iari-check
```

Only the release workflow adds `--apply`. Application verifies input/output hashes
and an exact state-JSON allowlist, installs files atomically one at a time, and rolls
back ordinary write failures. Nothing commits until installation and reconciliation
succeed. A killed job cannot commit a partially installed checkout. An unchanged
bulletin produces no commit or deployment. Deployment failure after a successful
commit still needs an explicit `deploy-existing` recovery run; it is not hidden by
the next unchanged ingestion. The shared release concurrency and both gates remain.

Each ingestion run retains raw evidence, candidate and validation report as a
14-day Actions artifact. These are public data; no credentials are included. A
source failure leaves the existing public site available at its last validated date.

`python tests/replay_iari_schedule.py` runs entirely offline in temporary folders.
It checks all 126 real recovered JSON hashes, an unchanged bulletin, a clearly
synthetic nonzero next day, full-season reconciliation, installation, idempotency,
missing-source and wrong-date failures. The validation workflow runs this before
release. Hosted execution of this new migration is still pending the authorized push.

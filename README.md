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
- `ingest-and-deploy`: retrieve today's data, reconcile it, commit only state
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

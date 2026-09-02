# twitter-import

Independent Twitter/X connector for FriendFeed. It never opens the FriendFeed
database and communicates only through the HTTPS import API.

## Archive import

```bash
go build ./cmd/twitter-import
./twitter-import inspect archive.zip
FF_FEED_API_KEY=... ./twitter-import import archive.zip \
  --endpoint https://friendfeed.example --state ./twitter-import.db --limit 100
```

Replies are skipped by default, matching the historical FriendFeed Twitter
import. Pass `--include-replies` to include replies. `--key-file` accepts a
mode-0600 file instead of the environment. Tokens and tweet bodies are never
written to logs or the state database.

## Collect recent posts with GetXAPI

GetXAPI access is paid, so collection is deliberately bounded to 100 posts per
user unless `--limit` is explicitly changed.

```bash
./twitter-import collect \
  --accounts-file ./twitter-users.tsv \
  --output ./output \
  --getxapi-key-file ./getx-api-key
```

The TSV maps immutable Twitter user IDs to canonical FriendFeed Feed UUIDs:

```text
feed_id  feed_uuid  twitter_username  twitter_user_id  boundary_tweet_id  boundary_at
alice    9e43d39c-2358-40a4-80ab-08a79a7b21e2  AliceX  42  100  2026-01-12T13:44:55Z
```

The optional boundary columns are a fixed snapshot exported from ffdb. Normal
sync stops at that tweet ID or timestamp; `--full` ignores the boundary.

`collect` calls GetXAPI's `/twitter/user/tweets` endpoint by numeric user ID,
expands `t.co` entities, downloads supported Twitter media, and writes one
`twitter-import/v1` ZIP per user plus a batch `manifest.json`. It does not
import anything. Use `--no-media` for a metadata-only canary. The GetXAPI key
may instead be provided through `GETXAPI_KEY`.

## Incremental synchronization

```bash
./twitter-import sync \
  --accounts-file ./twitter-users.tsv \
  --endpoint https://friendfeed.example \
  --getxapi-key-file ./getx-api-key \
  --operator-key-file ./operator-key
```

For each user, `sync` reads newest posts first and imports them sequentially.
When the TSV has a fixed boundary, only that boundary ends the scan; earlier
server replays are counted and traversal continues. Without a fixed boundary,
the first server replay ends the scan. It examines at most 100 posts per user
per run. The default output directory is `./output`; use `--output` to change it.
Each paid page is atomically saved before import as
`<twitter-user-id>/page-<first>-<last>.zip`. State and reports are stored as
`twitter-sync.json` and `twitter-sync.jsonl` in the same directory.
`manifest.json` is atomically rebuilt from retained page ZIPs after each
account, ready for production replay through the existing `batch` command.

An ordinary run always checks the newest page and preserves unfinished older
work. Use `--resume` to consume saved local ZIPs and their continuation cursors
first. Users without pending work start from their newest page normally, so
`--resume` does not skip first-time accounts. Multiple unfinished runs are kept
as a per-user stack, so a new latest check cannot overwrite an older gap. A
page that contains newly created entries is retained; a replay-only page is
removed. After a saved ZIP is exhausted, `--resume` follows its `NextCursor`
and may make new paid GetXAPI requests; it avoids duplicate reads but is not a
zero-cost mode.

Use `--full` only for an intentional full traversal. It removes the per-user
limit, clears pending incremental continuations, ignores replay boundaries,
and retains its page ZIPs; it may therefore incur substantial GetXAPI charges.
`--full` and `--resume` are mutually exclusive. Sync is sequential, has no hidden concurrency, and does not
retry paid GetXAPI reads automatically. FriendFeed import retries retain their
existing bounded backoff.

Content-level permanent failures are recorded as `rejected` and do not block
later posts or accounts. Media download failures are recorded as
`media_missing` and fall back to a text-only import. Credential failures and
exhausted temporary failures still stop the run without advancing that item.
This degradation applies only to `sync`; `collect` fails if media cannot be
downloaded because its purpose is to produce a complete offline bundle.
For multi-account sync, a GetXAPI 401/403/404 is recorded as
`account_unavailable` and skips that account. Any other account-level failure is
recorded as `sync_failed`; the remaining accounts still run, then the command
exits non-zero. If every account is unavailable, the command also exits non-zero
so a credential or plan failure cannot look successful.

The GetXAPI key and FriendFeed operator token are separate credentials. Both
files must be regular mode-0600 files. Neither credential is written to the
bundle, manifest, state, report, or logs.

### Replay tested data in production

Copy the complete output directory to production; no GetXAPI request or media
download is needed there. Validate first, then apply with a newly issued
short-lived production operator token:

```bash
./twitter-import batch ./output/manifest.json \
  --endpoint https://friendfeed.example

./twitter-import batch ./output/manifest.json \
  --endpoint https://friendfeed.example \
  --key-file ./operator-key --apply
```

The manifest uses `replay-state/<twitter-user-id>.db` checkpoints and the
shared `production-replay.jsonl` report under the copied output directory.
Development sync state is not reused. Re-running the batch is safe because the
server's deterministic import identity returns replay for existing entries.

## Bundle contract

```text
source.json   version, format=twitter-import/v1, collector, collection
items.jsonl   one item per line: id, account_id, time, text, relationships, URL, media paths
media/...     optional local media objects referenced by items.jsonl
```

## Operator batch import

An administrator can reuse one short-lived import-only operator token across
multiple target Feeds. The manifest defaults to validation; add `--apply` to
send entries.

```bash
./twitter-import batch ./output/manifest.json \
  --endpoint https://friendfeed.example

./twitter-import batch ./output/manifest.json \
  --endpoint https://friendfeed.example \
  --key-file ./operator-key --apply
```

Archive, state and report paths are relative to the manifest unless absolute.
Each import is sequential and stops on the first error. The operator token can
only call the import endpoint; it cannot read Feeds, publish ordinary Entries,
or perform administration.

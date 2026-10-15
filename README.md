# AxVaults — Multi-server

Fork of [Artillex-Studios/AxVaults](https://github.com/Artillex-Studios/AxVaults), based on 2.16.1.
Adds shared vault storage and ownership handoff between servers using Redis or MySQL.

**Downloads:** [Releases](https://github.com/SerhatDew/AxVaults/releases)

## Multi-server setup

Every participating server must use the same plugin version, shared storage and synchronization mode.
The proxy must forward the same player UUID on every server. Restart after changing backend settings.

### MySQL storage with SQL polling

```yaml
database:
  type: mysql
  address: localhost
  port: 3306
  database: axvaults
  username: axvaults
  password: "change-me"
multi-server-support: sql
```

Changes are announced through the shared database, polled every three seconds.
Ownership checks do not wait for this polling interval.

### Redis synchronization

Set `multi-server-support: redis` with either shared MySQL storage or `database.type: redis`.
Use the same Redis connection, logical database and channel on every participating server:

```yaml
database:
  type: redis
multi-server-support: redis
redis:
  host: localhost
  port: 6379
  password: "change-me"
  database: 0
  channel: "axvaults:sync"
```

When Redis stores vault contents, enable persistence, keep backups and use `maxmemory-policy noeviction`.
Do not use an evicting cache for persistent vault data or ownership locks.
H2 and SQLite remain available for single-server use with `multi-server-support: none`;
separate local databases cannot synchronize inventories through Redis.

## Ownership and failure handling

- One server owns a player's vaults at a time, including offline administrative access.
- A server saves before releasing ownership. The next server loads from shared storage after acquiring it.
- A competing request waits up to ten seconds, then fails rather than taking an active server's lock.
- Storage and lock failures deny access; failed saves retain the cached inventory and ownership.
- Remote notifications do not overwrite an owned inventory. This is not simultaneous editing across servers.
- Idle offline administrative inventories are saved and released after their viewers close.

Locks are intentionally persistent. After an unclean shutdown, stop the affected servers, inspect the
last saved inventory and remove only the stale lock for the affected UUID before restarting:

```sql
SELECT uuid, server, date FROM axvaults_locks WHERE uuid = '<player-uuid>';
DELETE FROM axvaults_locks WHERE uuid = '<player-uuid>' AND server = '<recorded-owner>';
```

For Redis, inspect `axvaults:lock:<player-uuid>` with `GET`, then delete that exact key with `DEL`.
Never clear locks while their owner may still be running, and never flush the vault database.
Unsaved changes cannot be recovered from a crashed process; restore from a backup if necessary.

## Upgrading and verification

Stop all participating servers and back up both storage and plugin configuration before upgrading.
Start one server first and let schema migration finish before starting the others.
Legacy MySQL tables gain the keys needed for atomic upserts. Identical duplicate rows can be collapsed;
conflicting inventories stop startup for manual reconciliation instead of choosing one and discarding the other.
Changing `database.type` does not migrate existing inventories to the new backend.

Build with Java 21 and Maven:

```sh
mvn clean verify
```

The distributable is `target/AxVaults.jar`. The multi-server release was exercised with two Paper 1.21.4
servers, real protocol clients, Redis 8.10.1 and MariaDB 11.4.5 through the MySQL connector.
Standalone H2 and SQLite save/reconnect flows and manual-save completion were also exercised.
Other Minecraft versions and Folia were not exercised in this release.
Update notifications follow this fork's GitHub releases, not upstream releases.

## Upstream

Original project links:

**Bug Reports and Feature Requests:** https://github.com/Artillex-Studios/Issues

**Support:** https://dc.artillex-studios.com/

![axvaults-banner](https://github.com/Artillex-Studios/AxVaults/assets/52270269/5a8197f8-623c-418d-95cf-4fb3e32c1daf)

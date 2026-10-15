# AxVaults

My fork of [Artillex-Studios/AxVaults](https://github.com/Artillex-Studios/AxVaults), based on 2.16.1. I've added shared vault storage through MySQL or Redis so players can use the same vaults across servers.

[Downloads](https://github.com/SerhatDew/AxVaults/releases)

## What's changed

- Shared vault contents across servers using MySQL or Redis.
- Synchronization through SQL polling or Redis Pub/Sub.
- Ownership locks so two servers cannot edit the same player's vaults at once, including offline admin access.
- Vaults are saved before ownership passes to another server.
- Storage or locking errors block access rather than opening an inventory that may be out of date.
- Players can take blacklisted items out of a vault, but cannot put them in.

This is not simultaneous editing. Only one server owns a player's vaults at a time.

## Multi-server setup

Use the same plugin version, shared storage, and synchronization mode on every participating server. Your proxy must forward the same player UUID to each server. Restart after changing backend settings.

### MySQL with SQL polling

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

Servers check the shared database for change notifications every three seconds. Ownership checks happen separately and do not wait for the polling interval.

### Redis synchronization

Set `multi-server-support: redis`. You can keep vault contents in shared MySQL storage, or use `database.type: redis` to store them in Redis too.

For Redis storage and synchronization:

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

Use the same Redis connection, logical database, and channel on all participating servers.

If Redis stores your vault contents, enable persistence, keep backups, and set `maxmemory-policy noeviction`. An evicting cache is not suitable for vault data or ownership locks.

H2 and SQLite still work for a single server with `multi-server-support: none`. Separate local databases cannot share vault contents just by enabling Redis synchronization.

## How vault ownership works

A server acquires ownership before loading a player's vaults from shared storage. It saves them before releasing ownership, so the next server can load the saved contents.

If another server already owns the vaults, access waits for up to ten seconds and then fails. It does not take over an active lock. Notifications from other servers do not overwrite an inventory that is currently owned.

Failed saves keep the cached inventory and its ownership. Offline admin inventories are saved and released once they are idle and their viewers have closed them.

### After a server crash

Locks do not expire automatically. After an unclean shutdown, you may need to remove a stale lock manually.

Stop the affected servers first and check the last saved inventory. For MySQL, inspect the lock and remove only the row for the affected player and recorded owner:

```sql
SELECT uuid, server, date FROM axvaults_locks WHERE uuid = '<player-uuid>';
DELETE FROM axvaults_locks WHERE uuid = '<player-uuid>' AND server = '<recorded-owner>';
```

For Redis, inspect `axvaults:lock:<player-uuid>` with `GET`, then remove that exact key with `DEL`.

Never remove a lock while its owner might still be running. Do not flush the vault database. Changes that were only in memory when the server crashed cannot be recovered; restore from a backup if needed.

## Upgrading

1. Stop all participating servers.
2. Back up the storage and plugin configuration.
3. Start one server and wait for the schema migration to finish.
4. Start the remaining servers.

Legacy MySQL tables get the keys needed for atomic upserts. Identical duplicate rows can be collapsed. If duplicate rows contain different inventories, startup stops so you can resolve them manually; the plugin does not pick one and discard the other.

Changing `database.type` does not move existing inventories to the new backend.

## Building and compatibility

Build with JDK 21 and Maven:

```sh
mvn clean verify
```

The plugin JAR is `target/AxVaults.jar`.

The multi-server release was tested on two Paper 1.21.4 servers with real protocol clients, Redis 8.10.1, and MariaDB 11.4.5 through the MySQL connector. Single-server H2 and SQLite save/reconnect flows and manual-save completion were also tested.

Other Minecraft versions and Folia were not tested for this release. Java 25 and Paper 26.2 support has not been verified here.

Update notifications check this fork's GitHub releases, not upstream releases.

## Upstream

Original plugin by Artillex Studios:

- [Source](https://github.com/Artillex-Studios/AxVaults)
- [Bug reports and feature requests](https://github.com/Artillex-Studios/Issues)
- [Discord](https://dc.artillex-studios.com/)

![AxVaults banner](https://github.com/Artillex-Studios/AxVaults/assets/52270269/5a8197f8-623c-418d-95cf-4fb3e32c1daf)

# Changelog

## [0.1.0] - 2026-11-09

First release.

- **ordito-raft**: Raft as a pure state machine, with no threads, I/O or clock. Leader election with
  pre-vote and check-quorum, log replication with pipelining, fast backtracking and probing, the
  current-term commit rule, and linearizable reads with ReadIndex.
- **ordito-sim**: a deterministic simulator. A whole cluster, its network (loss, duplication,
  reordering, partitions, bridges), disks, crashes, pauses and clock skew run in one thread from one
  seed. Swarm testing varies the environment per seed. Raft's safety properties are checked after every
  step; the client history is checked for linearizability at the end. Five deliberate bugs can be
  injected, and the simulator finds each of them.
- **ordito-check**: a linearizability checker (Wing and Gong's search with Lowe's memoization) with a
  key-value model, partitioned per key, that explains the operation it cannot place.
- **ordito-server**: loomkv replicated with Raft. Any Redis client works; followers send `MOVED` to the
  leader. A checksummed, fsynced log on disk; the leader's timestamp in every entry as the replicated
  clock, so TTLs expire identically on every node.
- **ordito-chaos**: Jepsen-style tests of real server processes: partitions through TCP proxies,
  `kill -9`, `SIGSTOP`, then a linearizability check.
- **Playground**: the simulator in the browser, compiled from the same Java code with TeaVM. It replays
  seeds exactly like the JVM, which CI checks.

## Simioj - Distribute Execution Environment

![](./doc/img/simioj-logo-only-sm.png)

Welcome to the Zimbra _Simioj_ project.   _Simioj_ is designed to be
an extensible framework that allows for the reliable execution of
tasks in a clustered environment.

Examples of the type of tasks that it could support include, but are
not limited to:

- A distributed, replicated key-value system the supports functional
  metadata updates via `PATCH` operations.
- Maintenance of per-resource queues of asynchronous operations with
  guarantees that only a single actor in the system will operate on
  that resource at one time.

## Work-in-Progress

This project is a work-in-progress.  This _README_ will be updated
when it is ready for public consumption.


## Organization

Major components in `zimbra.simioj`

- actor
    - Convert a `type` or `record` such that it can process messages
      in its own thread using a `core.async` channel as the mailbox.
- config
    - Load and merge configuration
    - Accessor functions for various configuration items
- discovery
    - Discover and maintain node topology
    - Notify interested parties regarding topology changes
- endpoint/http
    - HTTP interface for APIs and Cluster
- main
    - startup code
- raft/
    - server
        - Raft server protocol and core implementations
    - log
        - Raft log protocol and core implementations
    - statemachine
        - Raft statemachine protocol and core implementations
- util
    - Miscellaneous utility functions that don't belong anywhere else.

## Raft Consensus Algorithm

_Simioj_ uses an implementation of the
[Raft Consensus Algorithm](https://raftconsensus.github.io/) to
maintain a hierarchy of distributed state machines.

At the top level there will be a cluster-wide state machine that uses
the normal Raft election protocol.  The default implementation
provided will be used to maintain a pool of shards that is distributed
across the cluster.  The number of shards and the number of replicas
will be controlled via configuration.

Each "leader" shard and its associated replicas (followers) will form
a separate "micro" Raft cluster.  A default _logging_ state machine
implementation that allows for resource-level logging operations
will be provided.


## License

Copyright © 2015 Zimbra, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

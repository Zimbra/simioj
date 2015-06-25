# simioj

Simioj - Asynchronous Task Framework

## Installation

Download from http://example.com/FIXME

## Usage

FIXME: explanation

    $ java -jar simioj-0.1.0-standalone.jar [args]

## Options

FIXME: listing of options this app accepts.

## Examples

FIXME: add examples

## Organization

Major components defined in `zimbra.simioj`

- actor
    - Convert a `type` or `record` to process messages in its own
      thread using a `core.async` `chan` as the mailbox
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


## License

Copyright © 2015 Zimbra, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

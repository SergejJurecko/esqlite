Esqlite
=====

An Erlang nif library for sqlite3.

Introduction
-----------------

This library allows you to use the accelent sqlite engine from
erlang. The library is implemented as a nif library, which allows for
the fastest access to a sqlite database. This can be risky, as a bug
in the nif library or the sqlite database can crash the entire Erlang
VM. If you do not want to take this risk, it is always possible to
access the sqlite nif from a separate erlang node.

Special care has been taken not to block the scheduler of the calling
process. This is done by handling all commands from erlang within a
lightweight thread. The erlang scheduler will get control back when
the command has been added to the command-queue of the thread.


Fork changes
-----------------

- Uses a fixed threading model instead of a new thread for every db connection.
Init call sets number of threads, every open call can set which thread 
that particular sqlite connection should execute on.
- Added exec_script which will execute multiple statements at once and return proper values (not just ok).
- Supports the backup api 

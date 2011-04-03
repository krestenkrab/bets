Welcome to BDB
==============

This package consists of two interesting modules

* `bdb` which wraps Berkeley DB with operations for `insert`, `lookup`, etc.
   Keys and values are binaries throughout this API.
* `bets` (for Berkeley-ETS) is an ETS-like API which uses `bdb`; it uses `sext`
   for encoding the key part of the tuples stored, so matching with `bets:select/2`
   only scans relevant subsets accordingly.

Underneath the covers, the module bdb_nifs has the low-level API that
implements most of the Berkeley DB C-level API.


BSB/BETS is Copyright (C) 2011 by Trifork, and released under The
Apache License, Version 2.

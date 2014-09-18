StepsDB-alpha
David W. Jeske ( davidj  at g.m.a.i.l )
=============

StepsDB is a research database, experimenting with a write-optimized alternative to btrees
in the general family of LSM (log structured merge). 

The StepsDB MTree is a self-hosting LSM which lives directly on addressed blocks 
(either a single file or directly on a block device). This makes it different than LSMs 
such as LevelDB, RocksDB, or ROSE, which all sit atop a directory/btree-ish abstraction.
The goal here being to reduce write-amplification, efficiently support smaller block sizes,
avoid rewriting stable old data, among other improvements.



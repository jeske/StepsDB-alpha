StepsDB-alpha
=============

David W. Jeske ( davidj  at g.m.a.i.l )

StepsDB is currently a research write-optimized database released under the Apache 2.0 license. 

It's current purpose is experimenting with a unique data-structure for a range-split 
log-structured-merge-tree called the "MTree". To learn more about LSM:

   http://www.quora.com/How-does-the-Log-Structured-Merge-Tree-work

The StepsDB MTree is a self-hosting LSM which lives directly on addressed blocks 
(either a single file or directly on a block device). This makes it different than LSMs 
such as LevelDB, RocksDB, or ROSE, which all sit atop a directory/btree-ish abstraction.
The goal here being to reduce write-amplification, efficiently support smaller block sizes,
avoid rewriting stable old data, among other improvements.

It can be loaded/built in MonoDevelop, Xamarin, or Visual Studio 2010+. Tests are in NUnit.


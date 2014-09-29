
Migrating Tree (MTREE) - a general purpose block hosted LSM (Log Structured Merge)

Copyright (C) 2008-2014, David W. Jeske

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-----

MTree is a Log Structured Merge sorted-order key-value physical storage
algorithm. Like other LSM implementations, it's basic structure is to 
accumulate new key writes into both a Log and a sorted in-memory WorkingSegment.
When the WorkingSegment reaches a threshold size, it is written to disk
as a set of segments in a new "differential generation" ontop of existing data. 
All reads must consult all generations to reconstruct current data. This is 
both because adjacent keys may be spread across generations, and also because
keys are deleted not by removing them, but by writing tombstones to the newest
generation which indicate data for that key in older generations should be
ignored. 

During exact key lookups, some LSM implementations use bloom filters to skip
lookups into generations which are known not to contain that exact key. The
current MTree implementation does not have bloom filters, but it is compatible
with this strategy.

Writing new generations adds undesirable costs to all reads. To minimize
these costs, portions of generations are periodically merged together, to
minimize the number of layers for a given key-range. Within a generations,
segments are not allowed to overlap in key-range. Valid sets of key-overlapping
segments are selected in adjacent generations, and merged into a new version
of that keyrange for the oldest generation in the merge. 

Some existing LSM implementations (LevelDB, RocksDB, ROSE), put their LSM
segments and segment-pointers ontop of traditional btree-ish filesystems. 

MTree "self-hosts" it's own data directly atop a block device. This provides
efficient metadata-updates during merges, allows better efficiency for smaller
segment sizes, and removes the write amplification of storing data or
metadata in a filesystem or metadata-btree. 

-----

The most unique thing to understand about the MTree is the way it finds 
relevant LSM segments for a key-fetch. 

LevelDB keeps a big manifest file containing pointers-to-segments. This is 
simple, but it is also rewritten every time any segment changes. A Btree keeps 
pointers-to-blocks in btree index nodes. These nodes are recursively walked to 
find the leaf node for a key. 

MTree is simpler than LevelDB, and uses a recursive walk like a btree, but the 
recursion is a little trickier to understand because MTree LSM data is written in 
time-ordered "differential layers" not using in-place updates like in a btree. 

In the MTree, index-keys (pointers to segments) ride along inside the LSM just like data-keys. 
This means, for example, that deleting an index-key means writing a tombstone to the current 
working segment. Because writes are persisted in time-ordered generations, the LSM is walked 
in time-order to "reconstruct" valid key-order index-pointers. 

To walk them, we start at the in-memory working-segment. We walk all index-keys found
in order from most-recent to least-recent generation. We may find valid index keys,
or we may find tombstones deleting indx keys. We also look for both "leaf" index-keys, 
which point directly to a segment containing the desired data-keys; and "indirect" index-keys, 
which point to a segment which contains more index keys. This is analogous to btree index-nodes, 
but instead of the nodes being recursive, the *keys* are recursive. 

You can see an older un-optimized version of this algorithm inside RangemapManager_OLD.cs.  
INTERNAL_segmentWalkForNextKey() . It starts at the memory working segment. It looks for a key-after 
the key requested, and adds it to the recordsBeingAssembled list. (we say "assembled" because they
might be partial updates). After this, it looks for index-keys which could point to the key requested. 
Because of the time-ordered LSM, these keys need to be checked that they were not over-written or
tombstoned by already seen index keys. If there is a new legitimate index-key, we recurse on it. 

Here are some other pointers.

Bend/MTree/LayerManager/LayerManager.cs : The "supervisor" for the key-value store
   .flushWorkingSegment() flushes the in-memory working segment
   .performMerge() and .mergeSegments() do an LSM merge
   ._writeSegment() actually writes the segments

Bend/MTree/LayerManager/RootMetadata/RangemapManager.cs : manages mapping of segments
  .mapGenerationToRegion() writes the segment pointers 
  .INTERNAL_segmentWalkCursorSetupForNextKey_NonRecursive() does the heavy lifting of setting up a cursor
  .getRecord_LowLevel_Cursor() handles cursor walking and restart

Bend/MTree/LayerManager/MergeManager.cs : computes and scores MTree block merge candidates
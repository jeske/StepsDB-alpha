
// IScanner - an efficient bi-directional scanning interface for sorted collections (like BDSkipList)
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System.Collections.Generic;
using System;

namespace Bend
{
    // ----- aggregate interface for scannable dictionaries 
    
    // TODO: consider how this will need to be reworked to efficiently handle 
    //   next/prev on prefix-compressed data

    public interface IScanner<K>
    {
        // currently scans are always performed (>=,<=) on the lowest/highest 
        // endpoints, because you can use MatchTo() to decide if you want to produce the record
        // TODO: consider if we need to simplify and define (>,<) operations on endpoints
        bool MatchTo(K candidate);
        IComparable<K> genLowestKeyTest(); 
        IComparable<K> genHighestKeyTest();
    }
    public interface IScannable<K, V>
    {
        KeyValuePair<K, V> FindNext(IComparable<K> keytest,bool equal_ok);
        KeyValuePair<K, V> FindPrev(IComparable<K> keytest,bool equal_ok);
        IEnumerable<KeyValuePair<K, V>> scanForward(IScanner<K> scanner);   
        IEnumerable<KeyValuePair<K, V>> scanBackward(IScanner<K> scanner);
    }
    public interface IScannableDictionary<K, V> : IDictionary<K, V>, IScannable<K, V> { }

    public class ScanRange<K> : IScanner<K> 
    {
        IComparable<K> lowkey, highkey;
        IComparable<K> matchtest;
        bool scan_all = false;
        public ScanRange(IComparable<K> lowkey, IComparable<K> highkey, IComparable<K> matchtest) { // scan between low and high key
            this.lowkey = lowkey;
            this.highkey = highkey;
            this.matchtest = matchtest;
        }

        public ScanRange(IComparable<K> matchtest) { // scan all
            this.lowkey = new minKey();
            this.highkey = new maxKey();
            scan_all = true;
        }
        public bool MatchTo(K value) {
            return true;
        }
        public class maxKey : IComparable<K>
        {
            public int CompareTo(K val) {
                return 1;
            }
        }
        public class minKey : IComparable<K>
        {
            public int CompareTo(K val) {
                return -1;
            }
        }
        public static ScanRange<K> All() {
            return new ScanRange<K>(null);
        }
        public IComparable<K> genLowestKeyTest() {
            if (scan_all) {
                return new minKey();
            } else {
                return lowkey;
            }
        }
        public IComparable<K> genHighestKeyTest() {
            if (scan_all) {
                return new maxKey();
            } else {
                return highkey;
            }
        }
        public override String ToString() {
            return String.Format("ScanRange( {0} -> {1} )", lowkey, highkey);
        }
    }

    

}
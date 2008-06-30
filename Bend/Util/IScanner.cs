// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System.Collections.Generic;
using System;

namespace Bend
{
    // ----- aggregate interface for scannable dictionaries 
    
    // TODO: consider how this will need to be reworked to efficiently handle 
    //   next/prev on prefix-compressed data

    public interface IScanner<K>
    {
        bool MatchTo(K candidate);
        IComparable<K> genLowestKeyTest();
        IComparable<K> genHighestKeyTest();
    }
    public interface IScannable<K, V>
    {
        KeyValuePair<K, V> FindNext(IComparable<K> keytest);
        KeyValuePair<K, V> FindPrev(IComparable<K> keytest);
        IEnumerable<KeyValuePair<K, V>> scanForward(IScanner<K> scanner);
        IEnumerable<KeyValuePair<K, V>> scanBackward(IScanner<K> scanner);
    }
    public interface IScannableDictionary<K, V> : IDictionary<K, V>, IScannable<K, V> { }

    class ScanRange<K> : IScanner<K> where K : IComparable<K>
    {
        K lowkey, highkey;
        IComparable<K> matchtest;
        bool scan_all = false;
        public ScanRange(K lowkey, K highkey, IComparable<K> matchtest) { // scan between low and high key
            this.lowkey = lowkey;
            this.highkey = highkey;
            this.matchtest = matchtest;
        }

        public ScanRange(IComparable<K> matchtest) { // scan all
            this.lowkey = default(K);
            this.highkey = default(K);
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
    }

    

}
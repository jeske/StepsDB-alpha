// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

namespace Bend {

    /*
     * This class checks that the values fed through do not run out. If they do, it throws an
     * SortedSetExhaustedException
     */

    public class SortedSetExhaustedException : Exception {
        public object payload;
        public SortedSetExhaustedException(string note, object payload) : base(note){
            this.payload = payload;
        }
    }

    static class SortedExhaustedCheck {

        public static IEnumerable<KeyValuePair<K, V>> CheckExhausted<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one, 
            string debug_note = "", 
            object payload = null) where K : IComparable<K> {

            IEnumerator<KeyValuePair<K, V>> oneenum = one.GetEnumerator();
            
            bool one_hasmore = oneenum.MoveNext();
           
            while (one_hasmore) {
                yield return oneenum.Current;
                one_hasmore = oneenum.MoveNext();
            }
            throw new SortedSetExhaustedException("CheckExhausted ran out: " + debug_note, payload);
        }
    }

}
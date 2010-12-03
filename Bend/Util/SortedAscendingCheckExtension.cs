// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

namespace Bend {
    static class SortedAscendingCheck {

        public static IEnumerable<KeyValuePair<K, V>> CheckAscending<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one, string debug_note = "") where K : IComparable<K> {

            IEnumerator<KeyValuePair<K, V>> oneenum = one.GetEnumerator();

            KeyValuePair<K, V> last_value = new KeyValuePair<K, V>();
            bool one_hasmore = oneenum.MoveNext();
            bool has_first = false;
            
            while (one_hasmore) {
                if (has_first) {
                    int cmp_value = oneenum.Current.Key.CompareTo(last_value.Key);

                    if (cmp_value == 0) {
                        throw new Exception(String.Format("SortAscendingCheck found duplicate adjacent Keys prev:{0} cur:{1} - {2}",
                            last_value, oneenum.Current, debug_note));

                    }
                    if (cmp_value < 0) {
                        throw new Exception(String.Format("SortAscendingCheck found non-ascending Keys prev:{0} cur:{1} - {2}",
                            last_value, oneenum.Current, debug_note));
                    }
                }

                last_value = oneenum.Current;
                has_first = true;
                yield return oneenum.Current;
                one_hasmore = oneenum.MoveNext();
            }
        }
    }

}
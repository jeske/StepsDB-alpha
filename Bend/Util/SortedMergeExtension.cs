// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

namespace Bend
{
    static class SortedMergeExtension
    {

        public static IEnumerable<KeyValuePair<K, V>> MergeSort<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one,
            IEnumerable<KeyValuePair<K, V>> two) where K : IComparable<K> {
            return MergeSort<K, V>(one, two, false);
        }
        public static IEnumerable<KeyValuePair<K, V>> MergeSort<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one,
            IEnumerable<KeyValuePair<K, V>> two, bool dropRightDuplicates) where K : IComparable<K> {

            IEnumerator<KeyValuePair<K, V>> oneenum = one.GetEnumerator();
            IEnumerator<KeyValuePair<K, V>> twoenum = two.GetEnumerator();


            bool one_hasmore = oneenum.MoveNext();
            bool two_hasmore = twoenum.MoveNext();

            while (one_hasmore && two_hasmore) {
                int compareResult = oneenum.Current.Key.CompareTo(twoenum.Current.Key);
                KeyValuePair<K, V> val;
                if (compareResult < 0) {
                    // one < two
                    val = oneenum.Current;
                    one_hasmore = oneenum.MoveNext();
                    yield return val;
                } else if (compareResult > 0) {
                    // one > two
                    val = twoenum.Current;
                    two_hasmore = twoenum.MoveNext();
                    yield return val;
                } else if (compareResult == 0) {
                    // output either!  one == two  (we pick one)
                    val = oneenum.Current;
                    one_hasmore = oneenum.MoveNext();
                    if (dropRightDuplicates) {
                        two_hasmore = twoenum.MoveNext(); // drop the duplicate on the right                        
                    }
                    yield return val;
                }
            }

            while (one_hasmore) {
                yield return oneenum.Current;
                one_hasmore = oneenum.MoveNext();
            }
            while (two_hasmore) {
                yield return twoenum.Current;
                two_hasmore = twoenum.MoveNext();
            }
        }
    }

}

namespace BendTests {
    using Bend;

    using NUnit.Framework;

    public partial class A00_UtilTest
    {
        [Test]
        public void T00_SortedMerge() {
            // make two sorted lists
            SortedList<int, string> one = new SortedList<int, string>();
            SortedList<int, string> two = new SortedList<int, string>();
            int[] onedata = { 1, 3, 5, 7 };
            int[] twodata = { 2, 4, 7, 9, 10 };

            int[] verify_output = { 1, 2, 3, 4, 5, 7, 7, 9, 10 };
            int[] verify_output_uniq = { 1, 2, 3, 4, 5, 7, 9, 10 };

            // fill the lists

            foreach (int num in onedata) {
                one.Add(num, "one value: " + num);
            }
            foreach (int num in twodata) {
                two.Add(num, "two value: " + num);
            }

            // test non-unique merge
            {
                int i = 0;
                foreach (KeyValuePair<int, string> kvp in one.MergeSort(two)) {
                    Assert.AreEqual(verify_output[i], kvp.Key, "non-uniq saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output.Length, i);
            }

            // test unique merge
            {
                int i = 0;
                foreach (KeyValuePair<int, string> kvp in one.MergeSort(two, true)) {
                    Assert.AreEqual(verify_output_uniq[i], kvp.Key, "uniq saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output_uniq.Length, i);
            }



        }
    }

}
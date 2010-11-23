using System;
using System.Collections.Generic;

namespace Bend {

    struct ValuePair<T> {
        public readonly T Value1;
        public readonly T Value2;

        public ValuePair(T one, T two) {
            Value1 = one;
            Value2 = two;
        }

    }

    static class SortedIntersectExtension {

        public static IEnumerable<KeyValuePair<K, ValuePair<V>>> SortedIntersect<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one,
            IEnumerable<KeyValuePair<K, V>> two) where K : IComparable<K> {

            IEnumerator<KeyValuePair<K, V>> one_enum = one.GetEnumerator();
            IEnumerator<KeyValuePair<K, V>> two_enum = two.GetEnumerator();


            bool one_hasmore = one_enum.MoveNext();
            bool two_hasmore = two_enum.MoveNext();

            while (one_hasmore && two_hasmore) {
                int compareResult = one_enum.Current.Key.CompareTo(two_enum.Current.Key);
                KeyValuePair<K, ValuePair<V>> val;

                if (compareResult == 0) {
                    val = new KeyValuePair<K,ValuePair<V>>(one_enum.Current.Key,
                        new ValuePair<V>(one_enum.Current.Value, two_enum.Current.Value));
                    one_hasmore = one_enum.MoveNext();
                    two_hasmore = two_enum.MoveNext();
                    // NOTE: each key in each sequence must be unique, no duplicates!
                    yield return val;
                } if (compareResult < 0) {
                    // one < two
                    one_hasmore = one_enum.MoveNext();
                } if (compareResult > 0) {
                    // two > one
                    two_hasmore = two_enum.MoveNext();
                }
            }
        }        
    }
}



namespace BendTests {
    using Bend;

    using NUnit.Framework;

    public partial class A00_UtilTest {
        [Test]
        public void T00_SortedIntersect() {
            // make two sorted lists
            SortedList<int, string> one = new SortedList<int, string>();
            SortedList<int, string> two = new SortedList<int, string>();
            int[] onedata = { 2, 5, 7 };
            int[] twodata = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            int[] verify_output = { 2, 5, 7 };            

            // fill the lists

            foreach (int num in onedata) {
                one.Add(num, "one value: " + num);
            }
            foreach (int num in twodata) {
                two.Add(num, "two value: " + num);
            }

            // A test one->two intersect
            {
                int i = 0;
                foreach (KeyValuePair<int, ValuePair<string>> kvp in one.SortedIntersect(two)) {
                    Assert.AreEqual(verify_output[i], kvp.Key, "A saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output.Length, i);
            }

            // B test two->one intersect 
            {
                int i = 0;
                foreach (KeyValuePair<int, ValuePair<string>> kvp in two.SortedIntersect(one)) {
                    Assert.AreEqual(verify_output[i], kvp.Key, "B saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output.Length, i);
            }
        }
    }

}
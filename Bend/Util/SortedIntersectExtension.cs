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

    delegate int CompareDelegate<T>(T key1, T key2);

    static class SortedIntersectExtension {

        public static IEnumerable<KeyValuePair<K, ValuePair<V>>> SortedIntersect<K, V>(
            this IEnumerable<KeyValuePair<K, V>> one,
            IEnumerable<KeyValuePair<K, V>> two,
            CompareDelegate<K> compareFunc = null
            ) where K : IComparable<K> {

            IEnumerator<KeyValuePair<K, V>> one_enum = one.GetEnumerator();
            IEnumerator<KeyValuePair<K, V>> two_enum = two.GetEnumerator();


            bool one_hasmore = one_enum.MoveNext();
            bool two_hasmore = two_enum.MoveNext();

            K one_key, two_key;

            while (one_hasmore && two_hasmore) {
                int compareResult;
                one_key = one_enum.Current.Key;
                two_key = two_enum.Current.Key;
               
                if (compareFunc != null) {
                    compareResult = compareFunc(one_key, two_key);
                } else {
                    compareResult = one_key.CompareTo(two_key);
                }
                
                KeyValuePair<K, ValuePair<V>> val;


                // TODO: handle two matching values in a row! 

                if (compareResult == 0) {
                    val = new KeyValuePair<K,ValuePair<V>>(one_key,
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
            int[] onedata = { 0, 2, 5, 7, 10 };
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

        public static int intersectCompare(string v1,string v2) {
            return v1.Split('/')[1].CompareTo(v2.Split('/')[1]);
        }
    }
    public partial class ZZ_TODO_UtilTest {
        [Test]
        public void T00_SortedIntersectMatchFunc() {
            // make two sorted lists
            SortedList<string, string> one = new SortedList<string, string>();
            SortedList<string, string> two = new SortedList<string, string>();
            string[] onedata = { "a/1/r", "a/2/r", "a/3/r" };
            string[] twodata = { "b/1/v", "b/2/v", "b/4/v" };

            string[] verify_output1 = { "a/1/r", "a/2/r" };
            string[] verify_output2 = { "b/1/v", "b/2/v" };

            // fill the lists

            foreach (var num in onedata) {
                one.Add(num, "one value: " + num);
            }
            foreach (var num in twodata) {
                two.Add(num, "two value: " + num);
            }

            /*
            // A test one->two intersect
            {
                int i = 0;
                foreach (KeyValuePair<string, ValuePair<string>> kvp in one.SortedIntersect(two,intersectCompare)) {
                    Assert.AreEqual(verify_output[i], kvp.Key, "A saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output.Length, i);
            }

            // B test two->one intersect 
            {
                int i = 0;
                foreach (KeyValuePair<string, ValuePair<string>> kvp in two.SortedIntersect(one, intersectCompare)) {
                    Assert.AreEqual(verify_output[i], kvp.Key, "B saw: " + kvp.Value);
                    i++;
                }
                Assert.AreEqual(verify_output.Length, i);
            }
             * 
             */

            Assert.Fail();
        }
    }

}
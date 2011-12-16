// BDSkipList - a thread-safe bi-directional skiplist
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System;
using System.Text;
using System.Threading;

using System.Collections.Generic;

using System.Diagnostics;


namespace BendTests {
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A00_BDSkipListTest {
        class CustomComparableToString : IComparable<string> {
            string value;
            public CustomComparableToString(string value) {
                this.value = value;
            }

            public int CompareTo(string target) {
                return value.CompareTo(target);
            }
        }
        [Test]
        public void T00_SkipList() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                // check list size
                Assert.AreEqual(i, l.Count);
                l.Add(keylist[i], valuelist[i]);
            }

            // test basic access
            for (int i = 0; i < keylist.Length; i++) {
                Assert.AreEqual(valuelist[i], l[keylist[i]]);
            }

            // assure a missing key throws key exception
            {
                bool err = false;
                try {
                    int val = l["not present"];
                } catch (KeyNotFoundException) {
                    err = true;
                }
                Assert.AreEqual(true, err, "missing key should throw KeyValueException");
            }

            {
                KeyValuePair<string, int> rec;
                // FindNext("" || null) should return the first element
                rec = l.FindNext("", true);
                Assert.AreEqual("abc", rec.Key);
                rec = l.FindNext("", false);
                Assert.AreEqual("abc", rec.Key);

                // FindPrev(null) should return the last element
                rec = l.FindPrev(null, true);
                Assert.AreEqual("ghi", rec.Key);
                rec = l.FindPrev(null, false);
                Assert.AreEqual("ghi", rec.Key);

                // FindPrev("") should return nothing
                bool err = false;
                try { rec = l.FindPrev("", true); } catch { err = true; }
                Assert.AreEqual(true, err);


            }
            {
                // FindNext(K)
                KeyValuePair<string, int> next = l.FindNext("b", false);
                Assert.AreEqual("def", next.Key);
                Assert.AreEqual(2, next.Value);

                // FindPrev(K)
                KeyValuePair<string, int> prev = l.FindPrev("g", false);
                Assert.AreEqual("def", prev.Key);
                Assert.AreEqual(2, prev.Value);
            }
            {
                Assert.AreEqual("def", l.FindNext("def", true).Key, "find def, equal_ok=true");
                Assert.AreEqual("ghi", l.FindNext("def", false).Key, "find def, equal_ok=false");
                KeyValuePair<string, int> next = l.FindNext("def", false);
                Assert.AreEqual("ghi", next.Key, "findnext should be >, not >=");
                Assert.AreEqual(3, next.Value);
                // FindPrev(K) better be <, not <=
                KeyValuePair<string, int> prev = l.FindPrev("def", false);
                Assert.AreEqual("abc", prev.Key, "findprev should be <, not <=");
                Assert.AreEqual(1, prev.Value);

            }

            {
                // FindNext(IComparable<K>)\
                KeyValuePair<string, int> next = l.FindNext(new CustomComparableToString("b"), false);
                Assert.AreEqual("def", next.Key, "FindNext(IComparable<K>) incorrect key");
                Assert.AreEqual(2, next.Value);


                // FindPrev(IComparable<K>)
                KeyValuePair<string, int> prev = l.FindPrev(new CustomComparableToString("g"), false);
                Assert.AreEqual("def", prev.Key, "FindPrev(IComparable<K>) incorrect key");
                Assert.AreEqual(2, prev.Value);
            }

            // use the iterator
            {
                int pos = 0;
                foreach (KeyValuePair<string, int> kvp in l) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos < keylist.Length, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos++;
                }
                Assert.AreEqual(pos, keylist.Length, "iterator did not return all elements it should have");
            }

            // use an iterator over an empty skiplist
            {
                BDSkipList<string, int> l2 = new BDSkipList<string, int>();
                foreach (KeyValuePair<string, int> kvp in l2) {
                    Assert.Fail("empty list should not cause iteration");
                }
            }
        }

        [Test]
        public void T00_Skiplist_LongOrdering() {
            BDSkipList<long, string> l = new BDSkipList<long, string>();

            long[] testin = { 6, 7, 8, 4, 5 };
            long[] testout = { 4, 5, 6, 7, 8 };

            foreach (long val in testin) {
                l.Add(val, val.ToString());
            }
            
            List<long> output_order = new List<long>();
            foreach(var kvp in l.scanForward(ScanRange<long>.All())) {
                output_order.Add(kvp.Key);
            }

            Assert.AreEqual(testout, output_order.ToArray(), "longs should be sorted");
            
        }

        [Test]
        public void T00_Skiplist_Remove() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                Assert.AreEqual(i, l.Count, "list size mismatch during add");

                int count = 0;
                foreach (var elem in l) {
                    count++;
                }
                Assert.AreEqual(l.Count, count, "enumerator produced a different number of elements than count, during add");

                l.Add(keylist[i], valuelist[i]);
            }

            for (int i = 0; i < keylist.Length; i++) {
                Assert.AreEqual(keylist.Length - i, l.Count, "list size mismatch during remove");

                int count = 0;
                foreach (var elem in l) {
                    count++;
                }
                Assert.AreEqual(l.Count, count, "enumerator produced a different number of elements than count, during forward remove");

                l.Remove(keylist[i]);
            }

            // test removing in reverse order
            l = new BDSkipList<string, int>();
            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                Assert.AreEqual(i, l.Count, "list size mismatch during add");
                l.Add(keylist[i], valuelist[i]);
            }
            for (int i = keylist.Length - 1; i >= 0; i--) {
                Assert.AreEqual(i + 1, l.Count, "list size mismatch during remove");

                int count = 0;
                foreach (var elem in l) {
                    count++;
                }
                Assert.AreEqual(l.Count, count, "enumerator produced a different number of elements than count, during reverse remove");


                l.Remove(keylist[i]);
            }



        }



        [Test]
        public void T01_Skiplist_ScannableDictionaryInterface() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };
            IScannableDictionary<string, int> ld = l;

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                Assert.AreEqual(i, l.Count, "list size mismatch during add");
                Assert.AreEqual(i, ld.Count, "IScannableDictionary.Count size mismatch during add");
                l.Add(keylist[i], valuelist[i]);
            }

        }

        [Test]
        public void T01_SkipList_Scanning() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                // check list size
                Assert.AreEqual(i, l.Count);
                l.Add(keylist[i], valuelist[i]);
            }

            // use the scan iterator, forward
            {
                int pos = 1;  // start at 1 because of our "b" search key
                foreach (KeyValuePair<string, int> kvp in l.scanForward(new ScanRange<string>("b", "z", null))) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos < keylist.Length, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos++;
                }
                Assert.AreEqual(keylist.Length, pos, "scanForward() did not return all elements it should have");
            }

            // use the scan iterator, backward
            {
                int pos = 1;  // start at 1 because of our "e" search key
                foreach (KeyValuePair<string, int> kvp in l.scanBackward(new ScanRange<string>("", "e", null))) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos >= 0, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos--;
                }
                Assert.AreEqual(-1, pos, "scanBackward() did not return all elements it should have");
            }

            // check scanForward endpoint
            {
                int pos = 1;
                foreach (KeyValuePair<string, int> kvp in l.scanForward(new ScanRange<string>("b", "e", null))) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos < keylist.Length, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos++;
                }
                Assert.AreEqual(2, pos, "scanForward() did not return the correct set of elements");

            }

            // check scanBackward endpoint
            {
                int pos = 1;  // start at 1 because of our before "e" search key
                foreach (KeyValuePair<string, int> kvp in l.scanBackward(new ScanRange<string>("b", "e", null))) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos >= 0, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos--;
                }
                Assert.AreEqual(0, pos, "scanBackward() did not return all elements it should have");
            }

        }

        [Test]
        public void T01_SkipList_TestKeysValuesProperties() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                // check list size
                Assert.AreEqual(i, l.Count);
                l.Add(keylist[i], valuelist[i]);
            }

            var keys_out = new List<string>();
            keys_out.AddRange(l.Keys);
            Assert.AreEqual(keylist, keys_out.ToArray(), "keys don't match");

            var values_out = new List<int>();
            values_out.AddRange(l.Values);
            Assert.AreEqual(valuelist, values_out.ToArray(), "values don't match");

        }

        [Test]
        public void T02_SkipList_TestDuplicateKeys() {
            BDSkipList<string, int> l = new BDSkipList<string, int>();
            string[] keylist = { "abc", "def", "def" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            l.Add(keylist[0], valuelist[0]);
            l.Add(keylist[1], valuelist[1]);

            {
                int caught_exception = 0;
                try {
                    l.Add(keylist[2], valuelist[2]);

                } catch (ArgumentException) {
                    caught_exception = 1;
                }
                Assert.AreEqual(1, caught_exception, "duplicate key addition didn't trigger exception");
            }

            Assert.AreEqual(2, l.Count);
            int count = 0;
            foreach (var val in l) { count++; }
            Assert.AreEqual(2, count);
        }


        // this is used to compare user-defined classes which have CompareTo() operators
        // that say they are equal, but the classes are actually unique pointers
        public class UserDefinedClass : IComparable<UserDefinedClass> {
            String value;
            public UserDefinedClass(String value) {
                this.value = value;
            }
            public int CompareTo(UserDefinedClass target) {
                return this.value.CompareTo(target.value);
            }
        }

        [Test]
        public void T02_SkipList_TestUserDefinedDuplicateKeys() {
            BDSkipList<UserDefinedClass, int> l = new BDSkipList<UserDefinedClass, int>();
            UserDefinedClass[] keylist = { new UserDefinedClass("test1"), new UserDefinedClass("test2"), new UserDefinedClass("test2") };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            l.Add(keylist[0], valuelist[0]);
            l.Add(keylist[1], valuelist[1]);

            {
                int caught_exception = 0;
                try {
                    l.Add(keylist[2], valuelist[2]);

                } catch (ArgumentException) {
                    caught_exception = 1;
                }
                Assert.AreEqual(1, caught_exception, "duplicate key addition didn't trigger exception");
            }


            Assert.AreEqual(2, l.Count, "shouldn't be able to add duplicate user defined keys");
            int count = 0;
            foreach (var val in l) { count++; }
            Assert.AreEqual(2, count);
        }

        [Test]
        public void T01_SkipList_Scanningbigger() {
            BDSkipList<int, int> l = new BDSkipList<int, int>();
            for (int i = 0; i < 1000; i = i + 30) {
                l.Add(i, i);
            }

            // use PREVIOUS scan iterator
            {
                int last_val = 0xFFFFFFF;
                foreach (KeyValuePair<int, int> kvp in l.scanBackward(new ScanRange<int>(null))) {
                    Assert.AreEqual(true, last_val > kvp.Key, "keys should decrease");
                    last_val = kvp.Key;
                }
            }

        }

        public class SkipList_Threading_Tester {
            BDSkipList<string, int> list;
            int[] datavalues;

            int num_additions = 0;
            int num_retrievals = 0;
            int num_removals = 0;

            public SkipList_Threading_Tester(int num_values) {
                list = new BDSkipList<string, int>();
                Random rnd = new Random();
                datavalues = new int[num_values];
                for (int i = 0; i < num_values; i++) {
                    datavalues[i] = rnd.Next(0xfffff);
                }
            }

            public class threadLauncher {
                SkipList_Threading_Tester parent;
                int thread_num;
                public threadLauncher(SkipList_Threading_Tester parent, int thread_num) {
                    this.parent = parent;
                    this.thread_num = thread_num;
                }
                public void doVerify() {
                    this.parent.doVerify(this.thread_num);
                }
            }

            public void threadedTest(int numthreads) {
                List<Thread> threads = new List<Thread>();

                for (int threadnum = 0; threadnum < numthreads; threadnum++) {
                    threadLauncher launcher = new threadLauncher(this, threadnum);
                    Thread newthread = new Thread(new ThreadStart(launcher.doVerify));
                    threads.Add(newthread);
                }

                num_additions = 0; num_removals = 0; num_retrievals = 0;
                DateTime start = DateTime.Now;
                foreach (Thread th in threads) {
                    th.Start();
                }

                foreach (Thread th in threads) {
                    // rejoin the threads
                    th.Join();
                }
                double duration_ms = (DateTime.Now - start).TotalMilliseconds;
                double ops_per_sec = (num_additions + num_retrievals + num_removals) * (1000.0 / duration_ms);

                System.Console.WriteLine("SkipList Threading Test, {0} ms elapsed",
                    duration_ms);
                System.Console.WriteLine("  {0} additions, {1} retrievals, {2} removals",
                    num_additions, num_retrievals, num_removals);
                System.Console.WriteLine("  {0} ops/sec", ops_per_sec);

                int expected_count = numthreads * datavalues.Length;
                Assert.AreEqual(expected_count, num_additions, "addition count");
                Assert.AreEqual(expected_count, num_retrievals, "retrieval count");
                Assert.AreEqual(expected_count, num_removals, "removal count");

            }
            public void doVerify(int thread_num) {
                Random rnd = new Random(thread_num);
                Thread.Sleep(rnd.Next(500));
                // add the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string value = datavalues[i].ToString() + ":" + thread_num.ToString();
                    this.list[value] = datavalues[i];
                    Interlocked.Increment(ref num_additions);
                }

                // read the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string value = datavalues[i].ToString() + ":" + thread_num.ToString();
                    Assert.AreEqual(datavalues[i], list[value]);
                    Interlocked.Increment(ref num_retrievals);
                }

                // remove the values
                for (int i = 0; i < datavalues.Length; i++) {
                    string value = datavalues[i].ToString() + ":" + thread_num.ToString();
                    bool did_remove = this.list.Remove(value);
                    Assert.AreEqual(true, did_remove, "couldn't remove value: " + value);
                    if (did_remove) {
                        Interlocked.Increment(ref num_removals);
                    }
                }

            }
        }

        [Test]
        public void T10_Skiplist_Threading() {
            SkipList_Threading_Tester tester = new SkipList_Threading_Tester(100);
            // tester.doVerify(0);
            tester.threadedTest(200);
        }

    }

}
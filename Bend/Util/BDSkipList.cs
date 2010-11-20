
// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.
//
// originally inspired by the public source code:
// http://www.codersource.net/csharp_skip_list.aspx
//
// wikipedia explanation
// http://en.wikipedia.org/wiki/Skip_list
//
// A skip list is built in layers. The bottom layer is an ordinary ordered linked list. 
// Each higher layer acts as an "express lane" for the lists below, where an element in 
// layer i appears in layer i+1 with some fixed probability p (two commonly-used values 
// for p are 1/2 or 1/4). On average, each element appears in 1/(1-p) lists, and the tallest 
// element (usually a special head element at the front of the skip list) in \log_{1/p} n\, lists.
//
// head[3] 1
// head[2] 1------->4---->6
// head[1] 1---->3->4---->6------->9
// head[0] 1->2->3->4->5->6->7->8->9->10
//
// NOTE: this is space inefficient for small K,V because we allocate a linked list node 
// for EVERY K,V, plus every skiplist jump point.

// FURTHERMORE, this skiplist is extended to a doubly-linked skiplist, so we can traverse
// efficiently in either direction.



using System;
using System.Text;
using System.Threading;

using System.Collections.Generic;

using System.Diagnostics;

namespace Bend
{
    // For some notes about Debugger Visualizers.... 
    // http://msdn.microsoft.com/en-us/magazine/cc163974.aspx
    // http://www.4guysfromrolla.com/articles/011806-1.aspx
    [DebuggerDisplay(" SkipList- {ToString()}")]

    public class BDSkipList<K,V> : 
        IScannableDictionary<K,V>
        where K: IComparable<K> 
        // where V: IEquatable<V>
    {
        /* This "Node" class models a node in the Skip List structure. It holds a pair of (key, value)
         * and also three pointers towards Node situated to the right, upwards and downwards. */
        private class Node<K,V> where K: IComparable<K> 
        {

            private Node() {  // only we are allowed to call this
                this.left = this.right = this.up = this.down = null;
            }
            public Node(K key, V value)
                : this() {
                this.key = key;
                this.value = value;
                this.is_sentinel = false;
            }
            internal static Node<K, V> newSentinel() {
                Node<K, V> node = new Node<K, V>();
                node.is_sentinel = true;
                return node;
            }

            public K key;
            public V value;
            public bool is_sentinel;
            public Node<K,V> left, right, up, down;
        }

        /* Public constructor of the class. Receives as parameters the maximum number
         * of paralel lists and the probability. */
        public BDSkipList(int maxListsCount, double probability) {
            /* Store the maximum number of lists and the probability. */
            this.maxLevelHeight = maxListsCount;
            this.probability = probability;
            Clear();
        }

        public IEnumerable<KeyValuePair<K, V>> scanForward(IScanner<K> scanner) {
            IComparable<K> lowestKeyTest = null;
            IComparable<K> highestKeyTest = null;
            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            }

            Node<K, V> node = (lowestKeyTest != null) ? _findNextNode(lowestKeyTest,true) : this.head[0].right;
            
            while ((node != null) && (!node.is_sentinel) && 
                    ((highestKeyTest == null) || (highestKeyTest.CompareTo(node.key) >= 0)) ) 
            {
                if (scanner == null || scanner.MatchTo(node.key)) {
                    yield return new KeyValuePair<K, V>(node.key, node.value);
                }
                node = _findNext(node);
            }
        }

        public IEnumerable<KeyValuePair<K, V>> scanBackward(IScanner<K> scanner) {
            IComparable<K> highestKeyTest = null;
            IComparable<K> lowestKeyTest = null;

            if (scanner != null) {
                lowestKeyTest = scanner.genLowestKeyTest();
                highestKeyTest = scanner.genHighestKeyTest();
            }

            Node<K, V> node = (highestKeyTest != null) ? _findPrevNode(highestKeyTest,true) : this.tail[0].left;
            
            while ((node != null) && (!node.is_sentinel) && 
                    ((lowestKeyTest == null) || (lowestKeyTest.CompareTo(node.key) <= 0))) 
            {
                if (scanner==null || scanner.MatchTo(node.key)) {
                    yield return new KeyValuePair<K, V>(node.key, node.value);
                }
                node = _findPrev(node);
            }
        }

        /* This is another public constructor. It creates a skip list with 11 aditional lists
         * and with a probability of 0.5. */
        public BDSkipList() : this(11, 0.5) { }

        #region Add_Remove_Clear_Implementations
        // -------------------------[ Add  / Remove / Clear ]--------------------------------

        public void Clear() {
            // Initialize the sentinel nodes. We have a list of nodes on both end of the
            // skiplist, so we can start at either end and traverse
            lock (this) { 
                this.head = new Node<K, V>[maxLevelHeight + 1];
                this.tail = new Node<K, V>[maxLevelHeight + 1];

                for (int i = 0; i <= maxLevelHeight; i++) {
                    this.head[i] = Node<K, V>.newSentinel();
                    this.tail[i] = Node<K, V>.newSentinel();
                }

                /* Link the head sentinels of the lists one to another. Also link all head
                 * sentinels to the Singleton tail second sentinel. */
                for (int i = 0; i <= maxLevelHeight; i++) {
                    head[i].right = tail[i];
                    tail[i].left = head[i];
                    if (i > 0) {
                        head[i].down = head[i - 1];
                        head[i - 1].up = head[i];

                        tail[i].down = tail[i - 1];
                        tail[i - 1].up = tail[i];
                    }
                }

                /* For the beginning we have no additional list, only the bottom list. */
                this.currentListsCount = 0;

                this.nodeCount = 0;
            }
        }

        /* Adds a pair of (key, value) into the Skip List structure. */
        public void Add(K key, V value) {
            lock (this) {
                /* When inserting a key into the list, we will start from the top list and 
                 * move right until we find a node that has a greater key than the one we want to insert.
                 * At this moment we move down to the next list and then again right. 
                 * 
                 * In this array we store the rightmost node that we reach on each level. We need to
                 * store them because when we will insert the new key in the lists, it will be inserted
                 * after these rightmost nodes. 
                 *
                 * Because we are a BIDIRECTIONAL skip list, we do this once forwards, and once backwards
                 */

                // Forward from head
                Node<K, V>[] next = new Node<K, V>[maxLevelHeight + 1];

                /* Now we will parse the skip list structure, from the top list, going right and then down
                 * and then right again and then down again and so on. We use a "cursor" variable that will
                 * represent the current node that we reached in the skip list structure. */
                {
                    Node<K, V> cursor = head[currentListsCount];
                    for (int i = currentListsCount; i >= 0; i--) {
                        /* If we are not at the topmost list, then we move down with one level. */
                        if (i < currentListsCount)
                            cursor = cursor.down;

                        /* While we do not reach the tail sentinel and we do not find a greater 
                         * numeric value than the one we want to insert, keep moving right. */
                        while ((!cursor.right.is_sentinel) && (cursor.right.key.CompareTo(key) < 0))
                            cursor = cursor.right;

                        /* Store this rightmost reached node on this level. */
                        next[i] = cursor;
                    }
                }

                // Backward from tail
                Node<K, V>[] prev = new Node<K, V>[maxLevelHeight + 1];
                /* Now we will parse the skip list structure, from the top list, going left and then down
                 * and then left again and then down again and so on. We use a "cursor" variable that will
                 * represent the current node that we reached in the skip list structure. */
                {
                    Node<K, V> pcursor = tail[currentListsCount];
                    for (int i = currentListsCount; i >= 0; i--) {
                        /* If we are not at the topmost list, then we move down with one level. */
                        if (i < currentListsCount)
                            pcursor = pcursor.down;

                        /* While we do not reach the head sentinel and we do not find a lessor
                         * numeric value than the one we want to insert, keep moving left. */
                        while ((!pcursor.left.is_sentinel) && (pcursor.left.key.CompareTo(key) > 0))
                            pcursor = pcursor.left;

                        /* Store this rightmost reached node on this level. */
                        prev[i] = pcursor;
                    }
                }

                /* Here we are on the bottom list, and we test to see if the new value to add
                 * is not already in the skip list. If it already exists, then we just update
                 * the value of the key. */
                if ((!next[0].right.is_sentinel) && (next[0].right.key.Equals(key))) {
                    next[0].right.value = value;
                }

                    /* If the number to insert is not in the list, then we flip the coin and insert
                     * it in the lists. */
                else {
                    /* We find a new level number which will tell us in how many lists to add the new number. 
                     * This new random level number is generated by flipping the coin (see below). */
                    int newLevel = NewRandomLevel();

                    /* If the new level is greater than the current number of lists, then we extend our
                     * "next/prev nodes" array to include more head/tail pointers. 
                     * At the same time we increase the number of current lists. 
                     */
                    if (newLevel > currentListsCount) {
                        for (int i = currentListsCount + 1; i <= newLevel; i++) {
                            next[i] = head[i];
                            prev[i] = tail[i];
                        }
                        currentListsCount = newLevel;
                    }

                    /* Now we add the node to the lists and adjust the pointers accordingly. 
                     * We add the node starting from the bottom list and moving up to the next lists.
                     * When we get above the bottom list, we start updating the "up" and "down" pointer of the 
                     * nodes. */

                    // handle updating the right/left links at the same time
                    {
                        Node<K, V> prevNode = null;
                        Node<K, V> n = null;
                        for (int i = 0; i <= newLevel; i++) {
                            prevNode = n;
                            n = new Node<K, V>(key, value);
                            n.right = next[i].right;
                            n.left = prev[i].left;
                            next[i].right = n;
                            prev[i].left = n;
                            if (i > 0) {
                                n.down = prevNode;
                                prevNode.up = n;
                            }
                        }
                    }

                    nodeCount++; // increment our count of nods
                }
            }
        }

        /* This method computes a random value, smaller than the maximum admitted lists count. This random
         * value will tell us into how many lists to insert a new key. */
        private int NewRandomLevel() {
            int newLevel = 0;
            while ((newLevel < maxLevelHeight) && FlipCoin())
                newLevel++;
            return newLevel;
        }

        /* This method simulates the flipping of a coin. It returns true, or false, similar to a coint which
         * falls on one face or the other. */
        private bool FlipCoin() {
            double d = r.NextDouble();
            return (d < this.probability);
        }

        /* This method removes a fully matching key/value combo from the Skip List structure. 
         * If valuetest is null, only the key is considered
         */
        private bool _Remove(IComparable<K> keytest, IEquatable<V> valuetest) {
            lock (this) {
                /* To remove in this bidirectional skiplist, we find the  
                 * matching node, and then unlink it from both sides.
                 */

                Node<K, V> target = _findNextNode(keytest, true);

                if (target == null) {
                    // nothing to remove
                    return false;
                }

                if (keytest.CompareTo(target.key) != 0) {
                    // not equal
                    return false;
                }

                /* If we are doing valuematch, check the value of the node we found*/
                if (valuetest != null) {
                    if (!valuetest.Equals(target.value)) {
                        // values don't match, so we won't remove
                        return false;
                    }
                }

                // ---------------------------------------
                // REMOVE! - Now remove it from both sides.
               
                while (target != null) {
                    Node<K,V> left = target.left;
                    Node<K,V> right = target.right;
                    left.right = right;
                    right.left = left;

                    target = target.up;                    
                }

                nodeCount--;  // decrement our node counter
                return true;
            }
        }
        #endregion

        #region Internal_Find_Walk_GetEnumerator_Implementations
        // -----------------------[ internal find/walk implementations ] -------------------------


        Node<K, V> _findNextNode(IComparable<K> keytest, bool equal_ok) {
            lock (this) {
                /* We parse the skip list structure starting from the topmost list, from the head sentinel
                 * node. As long as we have keys smaller than the key we search for, we keep moving right.
                 * When we find a key that is greater or equal that the key we search, then we go down one
                 * level and there we try to go again right. When we reach the bottom list, we stop. */
                Node<K, V> cursor = head[currentListsCount];
                for (int i = currentListsCount; i >= 0; i--) {
                    if (i < currentListsCount) {
                        cursor = cursor.down;
                    }
                    if (equal_ok) {
                        while ((!cursor.right.is_sentinel) && (keytest != null) && (keytest.CompareTo(cursor.right.key) > 0)) {
                            cursor = cursor.right;
                        }
                    } else {
                        while ((!cursor.right.is_sentinel) && (keytest != null) && (keytest.CompareTo(cursor.right.key) >= 0)) {
                            cursor = cursor.right;
                        }
                    }
                }

                /* Here we are on the bottom list. Now we see if the next node is valid, if it is
                 * we return the node, if not, we return null.
                 */
                if (!cursor.right.is_sentinel) {
                    return cursor.right;
                } else {
                    return null;
                }
            }
        }
        Node<K, V> _findPrevNode(IComparable<K> keytest, bool equal_ok) {
            lock (this) {
                /* We parse the skip list structure starting from the topmost list, from the tail sentinel
                 * node. As long as we have keys bigger than the key we search for, we keep moving left.
                 * When we find a key that is less or equal to the key we're searching for, then we go down one
                 * level and there we try to go again left. When we reach the bottom list, we stop. */
                Node<K, V> pcursor = tail[currentListsCount];
                for (int i = currentListsCount; i >= 0; i--) {
                    if (i < currentListsCount) {
                        pcursor = pcursor.down;
                    }
                    if (equal_ok) {
                        while ((!pcursor.left.is_sentinel) && (keytest != null) && (keytest.CompareTo(pcursor.left.key) < 0)) {
                            pcursor = pcursor.left;
                        }
                    } else {
                        while ((!pcursor.left.is_sentinel) && (keytest != null) && (keytest.CompareTo(pcursor.left.key) <= 0)) {
                            pcursor = pcursor.left;
                        }
                    }
                }

                /* Here we are on the bottom list. Now we see if the next node is valid, if it is
                 * we return the node, if not, we return null.
                 */
                if (!pcursor.left.is_sentinel) {
                    return pcursor.left;
                } else {
                    return null;
                }
            }
        }

        Node<K, V> _findNext(Node<K, V> node) {
            lock (this) {
                // double check that we are in the bottom level "real" linked list
                while ((node != null) && (node.down != null)) {
                    node = node.down;
                }
                Node<K, V> next_node = node.right;
                // if it is the sentinel value, return null instead
                if (next_node.is_sentinel) {
                    return null;
                } else {
                    return next_node;
                }
            }
        }

        Node<K, V> _findPrev(Node<K, V> node) {
            lock (this) {
                // double check that we are in the bottom level "real" linked list
                while ((node != null) && (node.down != null)) {
                    node = node.down;
                }
                Node<K, V> prev_node = node.left;
                // if it is the sentinel value, return null instead
                if (prev_node.is_sentinel) {
                    return null;
                } else {
                    return prev_node;
                }
            }
        }


        Node<K, V> findNode(IComparable<K> keytest) {
            Node<K, V> next_node = _findNextNode(keytest, true);
            if (next_node != null) {
                if (keytest.CompareTo(next_node.key) == 0) {
                    return next_node;  // node match
                }
            }
            return null;
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator() {
            foreach (Node<K, V> cursor in this._internalEnumerator()) {
                yield return new KeyValuePair<K, V>(cursor.key, cursor.value);
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        private IEnumerable<Node<K, V>> _internalEnumerator() {
            Node<K, V> cursor = this.head[0];           

            // check that we are on the full-linked-list layer (i.e. bottom)
            if (cursor.down != null) {
                throw new Exception("Skiplist head[0] didn't bring us to the bottom, full-linked-list layer");
            }
            // move past the starting sentinel value
            if (cursor.is_sentinel) {
                cursor = cursor.right;
            } else {
                throw new Exception("Invalid head sentinel in SkipList");
            }            
            // now iterate across the linked list
            while (!cursor.is_sentinel) {                
                yield return cursor;

                if (cursor.right == cursor) {
                    throw new Exception("Invalid cyclic right pointer in SkipList");
                }
                cursor = cursor.right;
                
            }
            if (this.tail[0] != cursor) {
                throw new Exception("Invalid tail sentinel in SkipList");
            }
        }

        #endregion

        public bool Remove(IComparable<K> keytest) {
            return _Remove(keytest, null);
        }

        /* This method prints the content of the Skip List structure. It can be useful for debugging. */
        override public string ToString() {
            int values_included = 0;

            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<K,V> kvp in this) {
                if (values_included > 0) { sb.Append(", "); }
                sb.Append(kvp.Key.ToString());
                sb.Append("=>");
                sb.Append(kvp.Value.ToString());
                
                values_included++;
                if (values_included > 10) {
                    sb.Append("...");  // only include 10 values, then elide
                    break; 
                }
            }
            return sb.ToString();
        }

        #region SkipList_InstanceData

        /* This array will hold the first sentinel node from each list. */
        private Node<K,V>[] head;

        /* This node will represent the second sentinel for the lists. It is enough
         * to store only one sentinel node and link all lists to it, instead of creating
         * sentinel nodes for each list separately. */
        private Node<K,V>[] tail;

        /* This number represents the maximum number of lists that can be created. 
         * However it is possible that not all lists are created, depending on how
         * the coin flips. In order to optimize the operations we will not create all
         * lists from the beginning, but will create them only as necessary. */
        private int maxLevelHeight;

        /* This number represents the number of currently created lists. It can be smaller
         * than the number of maximum accepted lists. */
        private int currentListsCount;

        /* This number represents the probability of adding a new element to another list above
         * the bottom list. Usually this probability is 0.5 and is equivalent to the probability
         * of flipping a coin (that is why we say that we flip a coin and then decide if we
         * add the element to another list). However it is better to leave this probability
         * easy to change, because in some situations a smaller or a greater probability can
         * be better suited. */
        private double probability;

        private int nodeCount;  // the number of nodes we currently have

        /* This is a random number generator that is used for simulating the flipping of the coin. */
        private Random r = new Random();

        #endregion

        #region SkipList_FindNextFindPrev
        // ------------------------[ FindNext / FindPrev ]---------------------------

        public KeyValuePair<K, V> FindNext(IComparable<K> keytest, bool equal_ok) {
            Node<K, V> node = _findNextNode(keytest,equal_ok);
            if (node != null) {
                return new KeyValuePair<K, V>(node.key, node.value);
            } else {
                throw new KeyNotFoundException(
                  String.Format("FindNext(IComparable<{0}>({1})",
                    typeof(K).ToString(),
                    keytest.ToString()));

            }

        }

        public KeyValuePair<K, V> FindPrev(IComparable<K> keytest, bool equal_ok) {
            Node<K, V> node = _findPrevNode(keytest,false);
            if (node != null) {
                return new KeyValuePair<K, V>(node.key, node.value);
            } else {
                throw new KeyNotFoundException(
                  String.Format("FindNext(IComparable<{0}>({1})",
                    typeof(K).ToString(),
                    keytest.ToString()));

            }

        }

        #endregion


        public V this[K key] {
            get {
                Node<K,V> node = findNode(key);
                if (node != null) {
                    return node.value;
                } else {
                    throw new KeyNotFoundException("key not found: " + key.ToString());
                }
            }
            set {
                Node<K,V> node = findNode(key);
                if (node != null) {
                    node.value = value;
                } else {
                    Add(key, value);
                }
                
            }
        }


        #region IDictionary_mapping
        // ---------------------[ IDictionary ]-------------------------------
        public bool ContainsKey(K key) {
            Node<K,V> node = findNode(key);
            return (node != null);
        }
        public bool Remove(K key) {
            return Remove((IComparable<K>) key);
        }
        public bool TryGetValue(K key, out V value) {
            Node<K,V> node = findNode(key);
            if (node != null) {
                value = node.value;
                return true;
            } else {
                value = default(V);
                return false;
            }
        }
        public int Count {
            get { return this.nodeCount; }
        }

        // ------------ class KeysInterface --------------
        public class KeysInterface : ICollection<K>, IEnumerable<K> 
        {
            BDSkipList<K,V> target;
            internal KeysInterface(BDSkipList<K,V> target) {
                this.target = target;
            }
            public bool IsReadOnly { get { return true; } }
            public int Count {
                get {
                    return this.target.Count;
                }
            }

            public bool Contains(K key) {
                Node<K, V> node = target.findNode(key);
                return (node != null);
            }
            public void CopyTo(K[] keys, int start_index) {
                throw new Exception("TODO: KeysInterface<K>.CopyTo(..)");
            }


            #region ICollection_readonly_stubs
            public void Add(K key) {
                throw new Exception("ICollection.Add(..) called on read-only SkipList.KeysInterface");
            }
            public void Clear() {
                throw new Exception("ICollection.Clear() called on read-only SkipList.KeysInterface");
            }
            public bool Remove(K key) {
                throw new Exception("ICollection.Remove() called on read-only SkipList.KeysInterface");
            }
            #endregion

            public IEnumerator<K> GetEnumerator() {
                foreach (KeyValuePair<K,V> kvp in this.target) {
                    yield return kvp.Key;
                }
            }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
                return GetEnumerator();
            }
           
        }

        public ICollection<K> Keys {
            get { return new KeysInterface(this); } 
        }
        public ICollection<V> Values {
            get {
                Dictionary<V,bool> values = new Dictionary<V,bool>();
                foreach (KeyValuePair<K, V> kvp in this) {
                    values[kvp.Value] = true;
                }
                return values.Keys;
            }

        }

        public void Add(KeyValuePair<K, V> kvp) {
            this.Add(kvp.Key, kvp.Value);
        }
        public bool Contains(KeyValuePair<K, V> kvp) {
            V value;
            bool found = this.TryGetValue(kvp.Key, out value);
            if (found && kvp.Value.Equals(value)) {
                return true;
            } else {
                return false;
            }
        }
        public void CopyTo(KeyValuePair<K,V>[] dest_array, int start_index) {
            int cur_index = start_index;
            foreach (Node<K,V> skip_node in this._internalEnumerator()) {
                dest_array[cur_index++] = new KeyValuePair<K, V>(skip_node.key, skip_node.value);
            }
        }
        public bool Remove(KeyValuePair<K, V> matchingItem) {
            // TODO is it valid that we are only removing by key
            return Remove(matchingItem.Key);
        }
        public bool IsReadOnly { get { return false; } }

        #endregion


    }



}

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    
    public partial class A00_UtilTest
    {
        class CustomComparableToString : IComparable<string>
        {
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
                }
                catch (KeyNotFoundException) {
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
                try { rec = l.FindPrev("", true); }
                catch { err = true; }
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
                // FindNext(K) better be >, not >=
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
            for (int i = keylist.Length-1; i >= 0; i--) {                
                Assert.AreEqual(i+1, l.Count, "list size mismatch during remove");

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

        public class SkipList_Threading_Tester
        {
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

            public class threadLauncher
            {
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
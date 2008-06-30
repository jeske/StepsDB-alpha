
// originally taken from the public source code:
// http://www.codersource.net/csharp_skip_list.aspx

// wikipedia explanation
// http://en.wikipedia.org/wiki/Skip_list
//
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

// all improvements Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Text;

using System.Collections.Generic;

namespace Bend
{

    // ----- aggregate interface for scannable dictionaries 

    public interface IScannableDictionary<K, V> : IDictionary<K, V>, IScannable<K, V> { }

    // TODO: consider how this will need to be reworked to efficiently handle 
    //   next/prev on prefix-compressed data
    public interface IScannable<K, V>
    {
        KeyValuePair<K, V> FindNext(IComparable<K> keytest);
        KeyValuePair<K, V> FindPrev(IComparable<K> keytest);
        IEnumerable<KeyValuePair<K, V>> scanForward(IComparable<K> keytest);
        IEnumerable<KeyValuePair<K, V>> scanBackward(IComparable<K> keytest);
    }

    public class SkipList<K,V> : 
        IScannableDictionary<K,V>
        where K: IComparable<K> 
        where V: IEquatable<V>
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
        public SkipList(int maxListsCount, double probability) {
            /* Store the maximum number of lists and the probability. */
            this.maxLevelHeight = maxListsCount;
            this.probability = probability;
            Clear();
        }

        public IEnumerable<KeyValuePair<K, V>> scanForward(IComparable<K> keytest) {
            Node<K,V> node = _findNextNode(keytest);

            do {
                yield return new KeyValuePair<K, V>(node.key, node.value);
            } while ((node = _findNext(node)) != null);
        }

        public IEnumerable<KeyValuePair<K, V>> scanBackward(IComparable<K> keytest) {
            Node<K, V> node = _findPrevNode(keytest);

            do {
                yield return new KeyValuePair<K, V>(node.key, node.value);
            } while ((node = _findPrev(node)) != null);

        }

        /* This is another public constructor. It creates a skip list with 11 aditional lists
         * and with a probability of 0.5. */
        public SkipList() : this(11, 0.5) { }

        #region Add_Remove_Clear_Implementations
        // -------------------------[ Add  / Remove / Clear ]--------------------------------

        public void Clear() {
            // Initialize the sentinel nodes. We have a list of nodes on both end of the
            // skiplist, so we can start at either end and traverse
            
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

        /* Adds a pair of (key, value) into the Skip List structure. */
        public void Add(K key, V value) {
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

                    /* While we do not reach the tail sentinel and we do not find a lessor
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

                // handle both the "next" and prev case at the same time
                // First for the "forward" case
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

        public bool Remove(IComparable<K> keytest) {
            return Remove(keytest, null);
        }
        /* This method removes a fully matching key/value combo from the Skip List structure. 
         * If valuetest is null, only the key is considered
         */
        public bool Remove(IComparable<K> keytest, IEquatable<V> valuetest) {
            bool did_remove = false;
            /* For removal too we will search for the element to remove. While searching 
             * the element, we parse each list, starting from the top list and going down
             * towards the bottom list. On each list, we move from the first sentinel node
             * rightwards until either we reach the second sentinel node, either we find 
             * a node with a key greater or equal than the key we want to remove.
             * 
             * For each list we remember the rightmost node that we reached. */

            // -------------- first "forward" from the left ------------------------------
            Node<K, V>[] next = new Node<K, V>[maxLevelHeight + 1];

            /* We search for the value to remove. We start from the topmost list, from the first 
             * sentinel node and move right, down, right, down, etc.
             * As we said above, we will remember for each
             * level the rightmost node that we reach. */
            {
                Node<K, V> cursor = head[currentListsCount];
                for (int i = currentListsCount; i >= 0; i--) {
                    /* If we are not on the top level, then we move down one level. */
                    if (i < currentListsCount)
                        cursor = cursor.down;

                    /* Move right as long as we encounter values smaller than the value we want to
                     * remove. */
                    while ((!cursor.right.is_sentinel) && (keytest.CompareTo(cursor.right.key) > 0)) {
                        cursor = cursor.right;
                    }

                    /* If we are doing valuematch, move right as long as keys match but values don't */
                    if (valuetest != null) {
                        while ((!cursor.right.is_sentinel) &&
                            (keytest.CompareTo(cursor.right.key) == 0) &&
                            (!valuetest.Equals(cursor.right.value))) {
                            cursor = cursor.right;
                        }
                    }



                    /* When we got here, either we reached the second sentinel node on the current
                     * level, either we found a node that is not smaller than the value we want to
                     * remove. It is possible that the node we found is equal to the value that we
                     * want to remove, or it can be greater. In both cases we will store this
                     * rightmost node. */
                    next[i] = cursor;
                }
            }

            /* When we got here, we parsed even the bottom list and we stopped before a node
             * that is greater or equal with the value to remove. We test to see if it is equal
             * with the value or not. If it is equal, then we remove the value from the bottom
             * list and also from the lists above. */
            if ((!next[0].right.is_sentinel) && (keytest.CompareTo(next[0].right.key) == 0)) {
                /* Parse each existing list. */
                did_remove = true;

                for (int i = currentListsCount; i >= 0; i--) {
                    /* And if the rightmost reached node is followed by the key to remove, then
                     * remove the key from the list. */
                    if ((!next[i].right.is_sentinel) && (keytest.CompareTo(next[i].right.key) == 0)) {
                        next[i].right = next[i].right.right;
                    }
                }
            }


            // ----------------- second "backward" from the right ----------------------------

            Node<K, V>[] prev = new Node<K, V>[maxLevelHeight + 1];

            /* We search for the value to remove. We start from the topmost list, from the first 
             * sentinel node and move right, down, right, down, etc.
             * As we said above, we will remember for each
             * level the rightmost node that we reach. */
            {
                Node<K, V> pcursor = tail[currentListsCount];
                for (int i = currentListsCount; i >= 0; i--) {
                    /* If we are not on the top level, then we move down one level. */
                    if (i < currentListsCount)
                        pcursor = pcursor.down;

                    /* Move left as long as we encounter values bigger than the value we want to
                     * remove. */
                    while ((!pcursor.right.is_sentinel) && (keytest.CompareTo(pcursor.right.key) > 0)) {
                        pcursor = pcursor.left;
                    }

                    /* If we are doing valuematch, move left as long as keys match but values don't */
                    if (valuetest != null) {
                        while ((!pcursor.left.is_sentinel) &&
                            (keytest.CompareTo(pcursor.left.key) == 0) &&
                            (!valuetest.Equals(pcursor.left.value))) {
                            pcursor = pcursor.left;
                        }
                    }

                    /* When we got here, either we reached the second sentinel node on the current
                     * level, either we found a node that is not smaller than the value we want to
                     * remove. It is possible that the node we found is equal to the value that we
                     * want to remove, or it can be greater. In both cases we will store this
                     * rightmost node. */
                    prev[i] = pcursor;
                }
            }

            /* When we got here, we parsed even the bottom list and we stopped before a node
             * that is greater or equal with the value to remove. We test to see if it is equal
             * with the value or not. If it is equal, then we remove the value from the bottom
             * list and also from the lists above. */
            if ((!next[0].right.is_sentinel) && (keytest.CompareTo(next[0].right.key) == 0)) {
                /* Parse each existing list. */
                did_remove = true;

                for (int i = currentListsCount; i >= 0; i--) {
                    /* And if the rightmost reached node is followed by the key to remove, then
                     * remove the key from the list. */
                    if ((!next[i].right.is_sentinel) && (keytest.CompareTo(next[i].right.key) == 0)) {
                        next[i].right = next[i].right.right;
                    }
                }
            }



            if (did_remove) { nodeCount--; } // increment our node counter
            return did_remove;
        }
        #endregion

        #region Internal_Find_Walk_GetEnumerator_Implementations
        // -----------------------[ internal find/walk implementations ] -------------------------


        Node<K, V> _findNextPrevNode(IComparable<K> keytest, out Node<K,V> prev) {
           prev = null; // init previous pointer

            /* We parse the skip list structure starting from the topmost list, from the first sentinel
             * node. As long as we have keys smaller than the key we search for, we keep moving right.
             * When we find a key that is greater or equal that the key we search, then we go down one
             * level and there we try to go again right. When we reach the bottom list, we stop. */
            Node<K, V> cursor = head[currentListsCount];
            for (int i = currentListsCount; i >= 0; i--) {
                if (i < currentListsCount) {
                    cursor = cursor.down;
                }

                while ((!cursor.right.is_sentinel) && (keytest.CompareTo(cursor.right.key) > 0)) {
                    cursor = cursor.right;
                }
            }

            // record current value as previous
            // if prev is the sentinel, set to null
            prev = cursor;
            if (prev.key == null) { prev = null; }

            /* Here we are on the bottom list. Now we see if the next node is valid, if it is
             * we return the node, if not, we return null.
             */
            if (!cursor.right.is_sentinel) {
                return cursor.right;
            } else {
                return null;
            }
        }

        Node<K,V> _findNextNode(IComparable<K> keytest) {
            Node<K, V> prev;
            Node<K, V> next =_findNextPrevNode(keytest, out prev);
            return next;
        }
        Node<K, V> _findPrevNode(IComparable<K> keytest) {
            Node<K, V> prev;
            _findNextPrevNode(keytest, out prev);
            return prev;
        }
        Node<K, V> _findNode(IComparable<K> keytest) {
            Node<K,V> next_node = _findNextNode(keytest);
            if (next_node != null) {
                if (keytest.CompareTo(next_node.key) == 0) {
                    return next_node;  // node match
                } 
            }
            return null;
        }
        Node<K, V> _findNext(Node<K, V> node) {
            // double check that we are in the bottom level "real" linked list
            while ((node!=null) && (node.down!=null)) {
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

        Node<K, V> _findPrev(Node<K, V> node) {
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
                throw new Exception("Skiplist head[maxListsCount] didn't bring us to the bottom, full-linked-list layer");
            }
            // move past the starting sentinel value
            if (cursor.is_sentinel) {
                cursor = cursor.right;
            } else {
                throw new Exception("Invalid head sentinel in SkipList");
            }
            // now iterate across the linked list
            do {
                if (!cursor.is_sentinel) { // check for sentinel
                    yield return cursor;
                } else {
                    if (!cursor.is_sentinel) {
                        throw new Exception("Invalid tail sentinel in SkipList");
                    }
                }
                cursor = cursor.right;
            } while (!cursor.is_sentinel);

        }

        #endregion

        /* This method prints the content of the Skip List structure. It can be useful for debugging. */
        override public string ToString() {
            StringBuilder sb = new StringBuilder();
            sb.Append("current number of lists: " + currentListsCount);
            for (int i = currentListsCount; i >= 0; i--) {
                sb.Append(Environment.NewLine);
                Node<K,V> cursor = head[i];
                while (cursor != null) {
                    sb.Append("[");
                    if (cursor.key != null)
                        sb.Append(cursor.key.ToString());
                    else
                        sb.Append("N/A");
                    sb.Append(", ");
                    if (cursor.value != null)
                        sb.Append(cursor.value.ToString());
                    else
                        sb.Append("N/A");
                    sb.Append("] ");
                    cursor = cursor.right;
                }
            }
            sb.Append(Environment.NewLine);
            sb.Append("--------------");
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

        public KeyValuePair<K, V> FindNext(IComparable<K> keytest) {
            Node<K, V> node = _findNextNode(keytest);
            if (node != null) {
                return new KeyValuePair<K, V>(node.key, node.value);
            } else {
                throw new KeyNotFoundException(
                  String.Format("FindNext(IComparable<{1}>({2})",
                    typeof(K).ToString(),
                    keytest.ToString()));

            }

        }

        public KeyValuePair<K, V> FindPrev(IComparable<K> keytest) {
            Node<K, V> node = _findPrevNode(keytest);
            if (node != null) {
                return new KeyValuePair<K, V>(node.key, node.value);
            } else {
                throw new KeyNotFoundException(
                  String.Format("FindNext(IComparable<{1}>({2})",
                    typeof(K).ToString(),
                    keytest.ToString()));

            }

        }

        #endregion


        public V this[K key] {
            get {
                Node<K,V> node = _findNode(key);
                if (node != null) {
                    return node.value;
                } else {
                    throw new KeyNotFoundException("key not found: " + key.ToString());
                }
            }
            set {
                Node<K,V> node = _findNode(key);
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
            Node<K,V> node = _findNode(key);
            return (node != null);
        }
        public bool Remove(K key) {
            return Remove((IComparable<K>) key);
        }
        public bool TryGetValue(K key, out V value) {
            Node<K,V> node = _findNode(key);
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
            SkipList<K,V> target;
            internal KeysInterface(SkipList<K,V> target) {
                this.target = target;
            }
            public bool IsReadOnly { get { return true; } }
            public int Count {
                get {
                    return this.target.Count;
                }
            }

            public bool Contains(K key) {
                Node<K, V> node = target._findNode(key);
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
            // make sure we have a fully matching record
            return Remove(matchingItem.Key, matchingItem.Value);
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
            SkipList<string, int> l = new SkipList<string, int>();
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
                // FindNext(K)
                KeyValuePair<string, int> next = l.FindNext("b");
                Assert.AreEqual("def", next.Key);
                Assert.AreEqual(2, next.Value);

                // FindPrev(K)
                KeyValuePair<string, int> prev = l.FindPrev("g");
                Assert.AreEqual("def", prev.Key);
                Assert.AreEqual(2, prev.Value);
            }

            {
                // FindNext(IComparable<K>)\
                KeyValuePair<string, int> next = l.FindNext(new CustomComparableToString("b"));
                Assert.AreEqual("def", next.Key, "FindNext(IComparable<K>) incorrect key");
                Assert.AreEqual(2, next.Value);


                // FindPrev(IComparable<K>)
                KeyValuePair<string, int> prev = l.FindPrev(new CustomComparableToString("g"));
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
        }

        [Test]
        public void T01_SkipList_Scanning() {
            SkipList<string, int> l = new SkipList<string, int>();
            string[] keylist = { "abc", "def", "ghi" };
            int[] valuelist = { 1, 2, 3 };

            // put some data in 
            for (int i = 0; i < keylist.Length; i++) {
                // check list size
                Assert.AreEqual(i, l.Count);
                l.Add(keylist[i], valuelist[i]);
            }

            // use the scan iterator
            {
                int pos = 1;  // start at 1 because of our "b" search key
                foreach (KeyValuePair<string, int> kvp in l.scanForward("b")) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos < keylist.Length, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos++;
                }
                Assert.AreEqual(keylist.Length, pos, "scanForward() did not return all elements it should have");
            }

            // use the scan iterator
            {
                int pos = 1;  // start at 1 because of our "e" search key
                foreach (KeyValuePair<string, int> kvp in l.scanBackward("e")) {
                    // System.Console.WriteLine("skip[{0}] = {1}", kvp.Key, kvp.Value);
                    Assert.AreEqual(true, pos >= 0, "iterator returned too many elements");
                    Assert.AreEqual(keylist[pos], kvp.Key);
                    Assert.AreEqual(valuelist[pos], kvp.Value);
                    pos--;
                }
                Assert.AreEqual(-1, pos, "scanBackward() did not return all elements it should have");
            }

            
        }



    }

}
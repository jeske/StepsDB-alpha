
using System;
using System.Collections.Generic;

namespace Bend
{
    class LRUCache<K, V>
    {
        class _PriorityNode<NK, NV>
        {
            internal _PriorityNode<NK, NV> next;
            internal _PriorityNode<NK, NV> prev;
            internal K key;
            internal V value;
        }

        Dictionary<K, _PriorityNode<K, V>> data;
        _PriorityNode<K, V> head;
        _PriorityNode<K, V> tail;
        int maxEntries;

        public LRUCache(int maxEntries) {
            this.maxEntries = maxEntries;
            data = new Dictionary<K, _PriorityNode<K, V>>();
        }

        public V Get(K key) {
            _PriorityNode<K, V> node = data[key];

            // remove the node from the list
            _unlinkNode(node);

            // insert at the beginning
            _linkNode(node);

            return node.value;
        }

        public void Add(K key, V value) {
            _PriorityNode<K, V> node = new _PriorityNode<K, V>();
            node.key = key;
            node.value = value;
            _linkNode(node);
            data[node.key] = node;
            clean();
        }

        private void _linkNode(_PriorityNode<K, V> node) {
            // at beginning
            node.prev = null;
            node.next = head;
            head = node;
            if (tail == null) {
                tail = node;
            }
        }

        private void _unlinkNode(_PriorityNode<K, V> node) {
            _PriorityNode<K, V> prev = node.prev;
            _PriorityNode<K, V> next = node.next;
            
            if (prev != null) {
                prev.next = next;
            } else { head = next; }

            if (next != null) {
                next.prev = prev;
            } else { tail = prev; }
        }

        private void _removeOldest() {
            _PriorityNode<K, V> tailnode = tail;
            if (tailnode != null) {
                // System.Console.WriteLine("removing node: {0}", tailnode.key);
                _unlinkNode(tailnode);               
                
                bool did_remove = data.Remove(tailnode.key);
                if (!did_remove) {
                    // throw new Exception("LRUCache INTERNAL error, missing node to remove");
                }
            }
        }
        public void clean() {

            if (data.Count > this.maxEntries) {
                int removeCount = data.Count - this.maxEntries;
                // System.Console.WriteLine("clean : {0}, {1}, remove {2}", 
                //    this.maxEntries, data.Count, removeCount);
                
                for (int i = 0; i < removeCount; i++) {
                    _removeOldest();
                }
            }
        }

    }

}


namespace BendTests
{
    using NUnit.Framework;
    using Bend;

    public partial class A00_UtilTest
    {
        [Test]
        public void T00_LRUCache() {
            LRUCache<int, int> cache = new LRUCache<int, int>(3);

            cache.Add(1, 1);
            cache.Add(2, 2);
            cache.Add(3, 3);
            cache.Add(4, 4);
            
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));
            Assert.AreEqual(4, cache.Get(4));
            {
                bool err = false;
                try {
                    int val = cache.Get(1);
                }
                catch (KeyNotFoundException) {
                    err = true;
                }
                Assert.AreEqual(true, err, "oldest element should be flushed");
            }

        }

    }

}
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;

namespace ConsoleApplication1 {
    public class EnumeratorServerBatch<T> : IDisposable {
        IEnumerator<T> target;
        public EnumeratorServerBatch(IEnumerator<T> actual_enumerator) {
            target = actual_enumerator;
        }

        public List<T> getBatch() {
            Console.WriteLine("server getbatch");
            List<T> next_batch = new List<T>();
            int count = 0;
            while (target.MoveNext()) {
                next_batch.Add(target.Current);
                count++;
                if (count > 10) { break; }
            }
            return next_batch;
        }
        public void Reset() {
            target.Reset();
        }
        public void Dispose() {
            target.Dispose();
            target = null;
        }

    }

    public class EnumeratorClientBatch<T> : IEnumerator<T> {
        T cur;
        List<T> batch;
        EnumeratorServerBatch<T> batch_target;
        public EnumeratorClientBatch(EnumeratorServerBatch<T> target) {
            batch_target = target;
        }
        object IEnumerator.Current { get { return cur; } }
        public T Current { get { return cur; } }
        public bool MoveNext() {
            if (batch == null || batch.Count() == 0) {
                batch = batch_target.getBatch();
                if (batch.Count() == 0) {
                    return false;
                }
            }

            cur = batch[0]; batch.RemoveAt(0);
            return true;
        }
        public void Reset() {
            batch = new List<T>();
            batch_target.Reset();
        }
        public void Dispose() {
            batch = null;
            batch_target.Dispose();
            batch_target = null;
        }
    }

   
}

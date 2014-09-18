// FastUniqueIds - a fast way to generate unique "timestamp-ish" numbers
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System;

using System.Threading;

namespace Bend {
    public class FastUniqueIds {
        long current_timestamp = (_nowAsTime_T() << 16);
        private static long _nowAsTime_T() {
            DateTime start_time = new DateTime(1970, 1, 1);
            return (long)(DateTime.Now - start_time).TotalSeconds;
        }
        public long nextTimestamp() {
            return Interlocked.Increment(ref this.current_timestamp);
        }
    }
}

// RandomHelperExtension.Shuffle
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bend.RandomHelperExtension {
    public static class RandomHelper {
        public static void Shuffle<T>(this Random rnd, T[] array) {            
            for (int i=0;i<array.Length;i++) {
                int swap_with_index = rnd.Next(array.Length);
                T tmp_swap = array[i];
                array[i] = array[swap_with_index];
                array[swap_with_index] = tmp_swap;
            }
        }
    }
}

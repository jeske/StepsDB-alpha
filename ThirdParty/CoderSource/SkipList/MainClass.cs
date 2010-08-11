using System;
using System.Text;
using System.Collections;
using System.Collections.Specialized;

namespace SkipList
{
	class Class1
	{
		[STAThread]
		static void Main(string[] args)
		{
			/* We will test the skip list with many keys. */
			int n = 50000;

			/* Prepare the keys and the values. Make sure the keys are unique. */
			Console.WriteLine("Preparing {0} random unique numbers...", n);
			int[] uniqueNumbers = new int[n];
			Int32[] keys = new Int32[n];
			object[] values = new object[n];
			Random r = new Random();
			bool unique = true;
			int x = 0;
			for (int i=0; i<n; i++)
			{
				/* Each number we generate has to be unique. So after we generate a new
				 * number, we compare it with all previously generated numbers and if it is 
				 * now unique then we regenerate it. */
				do
				{
					x = r.Next();
					unique = true;
					for (int j=0; j<i; j++)
					{
						if (uniqueNumbers[j] == x)
						{
							unique = false;
							break;
						}
					}
				} while (!unique);
				uniqueNumbers[i] = x;

				StringBuilder sb = new StringBuilder();
				sb.AppendFormat("{0}", x);
				
				keys[i] = Int32.Parse(sb.ToString());
				values[i] = sb.ToString();
				
				Console.Write("\r{0}", i);
			}
			Console.WriteLine("\n{0} random numbers generated.", n);


			/* ------------------------------------------------
			 * Test and measure execution time for skip list. 
			 * ------------------------------------------------
			 */
			SkipList slist = new SkipList();

			DateTime start = DateTime.Now;

			/* Make insertions. */
			for (int i=0; i<n; i++)
				slist.Insert(keys[i], values[i]);

			/* Make searches. */
			for (int i=0; i<n; i++)
			{
				object obj = slist.Find(keys[i]);
				if (obj == null)
					throw new Exception("Null value for key ->"+keys[i]+"<-");

				string stringValue = (string)obj;
				if (!stringValue.Equals(values[i]))
					throw new Exception("Wrong value for key ->"+keys[i]+"<-");
			}

			/* Make removals. */
			for (int i=0; i<n; i++)
				slist.Remove(keys[i]);

			DateTime stop = DateTime.Now;

			Console.WriteLine("SkipList execution time: "+ (stop-start));

		
			/* ------------------------------------------------
			 * Test and measure execution time for standard Hashtable. 
			 * ------------------------------------------------
			 */
			SortedList stdSortedList = new SortedList();

			start = DateTime.Now;

			/* Make insertions. */
			for (int i=0; i<n; i++)
				stdSortedList.Add(keys[i], values[i]);

			/* Make searches. */
			for (int i=0; i<n; i++)
			{
				object obj = stdSortedList[keys[i]];
				if (obj == null)
					throw new Exception("Null value for key ->"+keys[i]+"<-");

				string stringValue = (string)obj;
				if (!stringValue.Equals(values[i]))
					throw new Exception("Wrong value for key ->"+keys[i]+"<-");
			}

			/* Make removals. */
			for (int i=0; i<n; i++)
				stdSortedList.Remove(keys[i]);

			stop = DateTime.Now;

			Console.WriteLine("Standard SortedList execution time: "+ (stop-start));
		}
	}
}

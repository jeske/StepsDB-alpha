using System;
using System.Collections;
using System.Text;
using System.Diagnostics;

using Analyzer = Lucene.Net.Analysis.Analyzer;
using StandardAnalyzer = Lucene.Net.Analysis.Standard.StandardAnalyzer;
using Document = Lucene.Net.Documents.Document;
using FilterIndexReader = Lucene.Net.Index.FilterIndexReader;
using IndexReader = Lucene.Net.Index.IndexReader;
using QueryParser = Lucene.Net.QueryParsers.QueryParser;
using FSDirectory = Lucene.Net.Store.FSDirectory;
using Version = Lucene.Net.Util.Version;
using Collector = Lucene.Net.Search.Collector;
using IndexSearcher = Lucene.Net.Search.IndexSearcher;
using Query = Lucene.Net.Search.Query;
using ScoreDoc = Lucene.Net.Search.ScoreDoc;
using Scorer = Lucene.Net.Search.Scorer;
using Searcher = Lucene.Net.Search.Searcher;
using TopScoreDocCollector = Lucene.Net.Search.TopScoreDocCollector;

namespace SearchFiles
{
    class BendSearchTest
    {

        [STAThread]
        public static void Main(System.String[] args)
        {
            System.String index = @"c:\EmailTest\LuceneDB";
            IndexReader reader = IndexReader.Open(FSDirectory.Open(new System.IO.FileInfo(index)), true); // only searching, so read-only=true
            Searcher searcher = new IndexSearcher(reader);

            if (Stopwatch.IsHighResolution) {
                System.Console.WriteLine("We have a high resolution timer with an frequency of {0} ticks/ms", Stopwatch.Frequency/1000);
            }

            searchFor(searcher, "jeske AND neotonic");
            searchFor(searcher, "noticed AND problems");
            searchFor(searcher, "data AND returned");
            searchFor(searcher, "scott AND hassan");

            searcher.Close();
            reader.Close();
            System.Console.WriteLine("done");
        }

  
        public class AnonymousClassCollector:Collector {
			private Scorer scorer;
			private int docBase;
            public ArrayList hitsarray = new ArrayList();
            public int Count { get { return hitsarray.Count; } }
			
			// simply print docId and score of every matching document
			public override void Collect(int doc) {
                string docid = doc + docBase + "";
                // System.Console.Out.WriteLine("doc=" + docid + " score=" + scorer.Score());
                hitsarray.Add(doc);
 			}
			
			public override bool AcceptsDocsOutOfOrder() {
				return true;
			}
			
			public override void  SetNextReader(IndexReader reader, int docBase) {
				this.docBase = docBase;
			}
			
			public override void  SetScorer(Scorer scorer) {
				this.scorer = scorer;
			}

 		}

        public static void searchFor(Searcher searcher, string querystr) {
            QueryParser parser = new QueryParser("body", new StandardAnalyzer());    // could be outside this function
            Query query = parser.Parse(querystr);

            var hits = new AnonymousClassCollector();

            // more accurate timer
            var timer = new Stopwatch();
            timer.Start();
			searcher.Search(query, hits);
            timer.Stop();

            Console.WriteLine("search for [{0}] returned {1} hits in {2}ms )",
                query, hits.Count, timer.ElapsedMilliseconds);
 
	    }
        
    }
}

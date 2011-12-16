using System;
using System.Text.RegularExpressions; // used to split body msg into words

using System.Collections.Generic;
using Bend;

using System.Security.Cryptography;


namespace Bend.Indexer {

    public class TextIndexer {
        LayerManager db;
        public readonly string index_location_prefix = ".zdata/index";

        int incrementing_docid = 1;

        SHA1CryptoServiceProvider sha1 = new SHA1CryptoServiceProvider();

        public TextIndexer(LayerManager db) {
            this.db = db;

        }

        public class IndexStats {
            public int unique_hits_produced = 0;
            public int entries_scanned = 0;
            public int comparisons = 0;
        }


        public void index_document(LayerWriteGroup txwg, string docid, List<String> wordlist) {
            //System.Console.WriteLine(msg.Body);
            int wordpos = 0;
            IDictionary<string, int> word_count = new BDSkipList<string, int>();            

            foreach (var word in wordlist) {
            // first find and count all the words

                // compute word counts
                if (!word_count.ContainsKey(word)) {
                    word_count[word] = 0;
                }
                word_count[word]++;

#if falase
                    // WORDHITS
                    var key = new RecordKey().appendParsedKey(index_location_prefix)
                       .appendKeyPart(word).appendKeyPart(docid).appendKeyPart("" + wordpos);
                    txwg.setValue(key, RecordUpdate.WithPayload(""));                
#endif

                // System.Console.WriteLine(key);                
                wordpos++;
            }


            // then insert the document
            // assign the docid string to a numeric id

            int doc_numeric_id = incrementing_docid++;
            txwg.setValue(new RecordKey().appendParsedKey(index_location_prefix)
                .appendKeyPart(new RecordKeyType_Long(doc_numeric_id)),
                RecordUpdate.WithPayload(docid));

            // now add all the words for that document
            foreach (var wordinfo in word_count) {
                var key = new RecordKey()
                    .appendParsedKey(index_location_prefix)                      // prefix
                    .appendKeyPart(wordinfo.Key)                                 // word itself
                    .appendKeyPart(new RecordKeyType_Long(doc_numeric_id));      // docid

                        txwg.setValue(key, RecordUpdate.WithPayload("" + wordinfo.Value));  // value is word-frequeny in the doc                    
            }
            
        }
        


        public class EndPrefixMatch : IComparable<RecordKey> {
            RecordKey key;
            public EndPrefixMatch(RecordKey k) {
                this.key = k;
            }
            public int CompareTo(RecordKey target) {
                if (target.isSubkeyOf(key)) {
                    return -1;
                } else {
                    return 1;
                }
            }
            public override string ToString() {
                return "EndPrefixMatch{" + key.ToString() + "}";
            }
        }
        

        public class TermDocHit {
            public string word;
            public long docid;            

            public override string ToString() {
                return String.Format("{0} in docid{1}", word, docid);
            }
        }

        public interface Term {
            TermDocHit advanceTo(IndexStats stats, TermDocHit newpos);
            TermDocHit advancePastDocid(IndexStats stats, long docid);
        };

        public class TermWord : Term {
            string word;           
            TextIndexer index;
            IEnumerator<KeyValuePair<RecordKey, RecordData>> hitlist;
            public TermWord(TextIndexer indexer, string word) {
                this.word = word;
                this.index = indexer;

                var start = new RecordKey()
                    .appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word);
                var end = RecordKey.AfterPrefix(new RecordKey()
                    .appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word));


                this.hitlist = index.db.scanForward(new ScanRange<RecordKey>(start, end, null)).GetEnumerator();

            }

            public IEnumerable<TermDocHit> allOccurances() {
   
                // <index prefix>.../<word>/<docid>

                var start = new RecordKey().appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word);
                var end = new EndPrefixMatch(new RecordKey().appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word));

                foreach (var hitrow in index.db.scanForward(new ScanRange<RecordKey>(start, end, null))) {
                    yield return unpackHit(hitrow.Key);
                }
            }
            private TermDocHit unpackHit(RecordKey hitrow) {
                // Console.WriteLine("unpackHit: " + hitrow);

                int len_of_index_prefix = 2;  // ASSUME THIS FOR NOW
                TermDocHit hit = new TermDocHit();
                hit.word = ((RecordKeyType_String)hitrow.key_parts[len_of_index_prefix + 0]).GetString();
                hit.docid = ((RecordKeyType_Long)hitrow.key_parts[len_of_index_prefix + 1]).GetLong();               
                return hit;
            }
            public TermDocHit advancePastDocid(IndexStats stats, long docid) {
                bool have_next = hitlist.MoveNext();
                while (have_next) {
                    KeyValuePair<RecordKey,RecordData> row = hitlist.Current;
                    TermDocHit hit = unpackHit(row.Key);

                    if (hit.word.CompareTo(this.word) != 0) {
                        throw new Exception(String.Format("invalid hit returned. word({0}) hit({1})",
                            this.word, hit));
                    }
                    stats.entries_scanned++;

                    if (hit.docid.CompareTo(docid) > 0) {
                        stats.unique_hits_produced++;
                        return hit;
                    }

                    have_next = hitlist.MoveNext();
                }
                throw new KeyNotFoundException(
                    String.Format("advancePastDocid({0}): no more hits for {1}", docid, this.word));
                
            }

            public TermDocHit advanceTo(IndexStats stats, TermDocHit newpos) {
                //    ".zindex/index/<word>/<docid>"
                bool have_next = hitlist.MoveNext();
                while (have_next) {
                    KeyValuePair<RecordKey, RecordData> row = hitlist.Current;
                    TermDocHit hit = unpackHit(row.Key);

                    if (hit.word.CompareTo(this.word) != 0) {
                        throw new Exception(String.Format("invalid hit returned. word({0}) hit({1})",
                            this.word, hit));
                    }

                    stats.entries_scanned++;

                    if (hit.docid.CompareTo(newpos.docid) >= 0) {
                        stats.unique_hits_produced++;
                        return hit;
                    }

                    have_next = hitlist.MoveNext();
                }
                throw new KeyNotFoundException(
                    String.Format("advanceTo({0}): no more hits for {1}", newpos.docid, this.word));

            }
        }

        public class TermAnd : Term {
            Term term1;
            Term term2;

            TermDocHit hit1;
            TermDocHit hit2;                       

            public TermAnd(Term left, Term right) {
                this.term1 = left;
                this.term2 = right;                
            }

            public TermDocHit advanceTo(IndexStats stats, TermDocHit hit) {
                return advancePastDocid(stats,hit.docid);
            }

            public TermDocHit advancePastDocid(IndexStats stats, long docid) {
                hit1 = term1.advancePastDocid(stats, docid);
                hit2 = term2.advancePastDocid(stats, docid);
                try {
                    while (true) {
                        stats.comparisons++;
                        switch (hit1.docid.CompareTo(hit2.docid)) {

                                // TODO fix this -1 crap, it's not valid, we need to check < 0 only
                            case -1:
                                // System.Console.WriteLine("     advance1: {0} < {1}", hit1, hit2);
                                hit1 = term1.advanceTo(stats, hit2);
                                break;
                            case 1:
                                // System.Console.WriteLine("     advance2: {0} > {1}", hit1, hit2);
                                hit2 = term2.advanceTo(stats, hit1);
                                break;
                            case 0:
                                // System.Console.WriteLine("        MATCH: {0} == {1}", hit1, hit2);                                                                
                                return hit1;
                        }
                    }
                } catch (KeyNotFoundException) {
                    // done finding hits
                    throw new KeyNotFoundException();
                }
            }
        }

        public List<long> HitsForExpression(IndexStats stats, Term term) {
            List<long> hits = new List<long>();
            TermDocHit hit = new TermDocHit();            
            hit.docid = 0;
            while (true) {
                try {
                    hit = term.advancePastDocid(stats,hit.docid);
                    hits.Add(hit.docid);
                    //System.Console.WriteLine("search returned: {0}", hit);
                } catch (KeyNotFoundException) {
                    //System.Console.WriteLine("search exception: " + e.ToString());
                    break;
                }
            }
            return hits;
        }


        public void searchFor(string expression) {
            String[] parts = Regex.Split(expression, @"\s");

            Term tree = null;

            foreach (var part in parts) {
                if (tree == null) {
                    tree = new TermWord(this,part);
                } else {
                    tree = new TermAnd(tree, new TermWord(this,part));
                }
            }
            if (tree == null) { 
                Console.WriteLine("empty search");
                return; 
            }
            var stats = new IndexStats();
            DateTime start = DateTime.Now;
            List<long> hits = HitsForExpression(stats, tree);
            double elapsed_s = (DateTime.Now - start).TotalMilliseconds/1000.0;
            Console.WriteLine("search for [{0}] returned {1} hits in {2}s   ({3} productions, {4} comparisons, {5} entries scanned)", 
                expression, hits.Count, elapsed_s, stats.unique_hits_produced, stats.comparisons, stats.entries_scanned);
            
            // Console.WriteLine("    " + String.Join(",",hits.Count < 15 ? hits : hits.GetRange(0,15)));
            //foreach (var hit in hits) {
            //    Console.WriteLine("     " + hit);
            //}
            
        }

        public void find_email_test() {

            System.Console.WriteLine("### In email test");

            System.Console.WriteLine("### term intersection ");

            searchFor("jeske neotonic");
            searchFor("noticed problems");
            searchFor("data returned");
            searchFor("scott hassan");
            searchFor("and the with not about jeske");
        }

    }


}
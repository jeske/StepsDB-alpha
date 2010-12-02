using System;
using System.Text.RegularExpressions; // used to split body msg into words

using System.Collections.Generic;
using Bend;


namespace Bend.Indexer {

    public class TextIndexer {
        LayerManager db;
        public readonly string index_location_prefix = ".zdata/index";

        public TextIndexer(LayerManager db) {
            this.db = db;

        }


        public void index_document(LayerManager.WriteGroup txwg, string docid, string txtbody) {
            //System.Console.WriteLine(msg.Body);
            int wordpos = 0;

            foreach (var possibleword in Regex.Split(txtbody, @"[-*()\""'[\]:\s?.,]+")) {
                String srcword = possibleword;
                srcword = Regex.Replace(srcword, @"([-""':+_=\/|]{3,})", "");

                if (srcword.Length == 0) { continue; }
                srcword = srcword.ToLower();
                // System.Console.Write(srcword + "/");

                // clean up word.
                var word = srcword.ToLower();
                // remove 's , do stimming, ignore non-words.

                // create a key and insert into the db
                // TODO: docid may have / on UNIX .
                var key = new RecordKey().appendParsedKey(index_location_prefix)
                    .appendKeyPart(word).appendKeyPart(docid).appendKeyPart("" + wordpos);

                // System.Console.WriteLine(key);
                txwg.setValue(key, RecordUpdate.WithPayload(""));
                wordpos++;
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
        public class IndexStats {
            public int unique_hits_produced = 0;
            public int comparisons = 0;
        }


        public class TermHit {
            public string word;
            public string docid;
            public string position;

            public override string ToString() {
                return String.Format("{0} at ({1} : {2})", word, docid, position);
            }
        }

        public interface Term {
            TermHit advanceTo(IndexStats stats, TermHit newpos);
            TermHit advancePastDocid(IndexStats stats, string docid);
        };

        public class TermWord : Term {
            string word;           
            TextIndexer index;
            public TermWord(TextIndexer indexer, string word) {
                this.word = word;
                this.index = indexer;
            }

            public IEnumerable<TermHit> allOccurances() {
                // <index prefix>.../<word>/<docid>/<position>
                var start = new RecordKey().appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word);
                var end = new EndPrefixMatch(new RecordKey().appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(word));

                foreach (var hitrow in index.db.scanForward(new ScanRange<RecordKey>(start, end, null))) {
                    yield return unpackHit(hitrow.Key);
                }
            }
            private TermHit unpackHit(RecordKey hitrow) {
                // Console.WriteLine("unpackHit: " + hitrow);

                int len_of_index_prefix = 2;  // ASSUME THIS FOR NOW
                TermHit hit = new TermHit();
                hit.word = ((RecordKeyType_String)hitrow.key_parts[len_of_index_prefix + 0]).GetString();
                hit.docid = ((RecordKeyType_String)hitrow.key_parts[len_of_index_prefix + 1]).GetString();
                hit.position = ((RecordKeyType_String)hitrow.key_parts[len_of_index_prefix + 2]).GetString();
                return hit;
            }
            public TermHit advancePastDocid(IndexStats stats, string docid) {
                var prefix = new RecordKey().appendParsedKey(this.index.index_location_prefix)
                      .appendKeyPart(this.word)
                      .appendKeyPart(docid);
                var keysearch = RecordKey.AfterPrefix(prefix);                   

                KeyValuePair<RecordKey, RecordData> row = index.db.FindNext(keysearch, false);
                TermHit hit = unpackHit(row.Key);

                if (hit.word.CompareTo(this.word) != 0) {
                    throw new KeyNotFoundException(
                        String.Format("advancePastDocid({0}): no more hits for {1}", docid, this.word));
                }

                if (hit.docid.CompareTo(docid) <= 0) {
                    throw new Exception(
                        String.Format("INTERNAL ERROR: failure to advance past docid({0}) prefix({1}) rowreturned({2})",
                              docid, prefix, row.Key));
                }
                stats.unique_hits_produced++;
                return hit;
            }

            public TermHit advanceTo(IndexStats stats, TermHit newpos) {
                //    ".zindex/index/<word>/<docid>"

                var keysearch = new RecordKey().appendParsedKey(this.index.index_location_prefix)
                                      .appendKeyPart(this.word)
                                      .appendKeyPart(newpos.docid)
                                      .appendKeyPart(newpos.position);

                KeyValuePair<RecordKey, RecordData> row = index.db.FindNext(keysearch, false);
                TermHit hit = unpackHit(row.Key);
                if (hit.word.CompareTo(this.word) == 0) {
                    stats.unique_hits_produced++;
                    return hit;
                } else {
                    throw new KeyNotFoundException(
                        String.Format("advanceTo({0}): no more hits for {1}", newpos, this.word));
                }
            }
        }

        public class TermAnd : Term {
            Term term1;
            Term term2;

            TermHit hit1;
            TermHit hit2;                       

            public TermAnd(Term left, Term right) {
                this.term1 = left;
                this.term2 = right;                
            }

            public TermHit advanceTo(IndexStats stats, TermHit hit) {
                return advancePastDocid(stats,hit.docid);
            }

            public TermHit advancePastDocid(IndexStats stats, string docid) {
                hit1 = term1.advancePastDocid(stats,docid);
                hit2 = term2.advancePastDocid(stats, docid);
                try {
                    while (true) {
                        stats.comparisons++;
                        switch (hit1.docid.CompareTo(hit2.docid)) {
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

        public List<string> HitsForExpression(IndexStats stats, Term term) {
            List<string> hits = new List<string>();
            TermHit hit = new TermHit();            
            hit.docid = "";
            while (true) {
                try {
                    hit = term.advancePastDocid(stats,hit.docid);
                    hits.Add(hit.docid);
                    //System.Console.WriteLine("search returned: {0}", hit);
                } catch (KeyNotFoundException e) {
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
            List<string> hits = HitsForExpression(stats, tree);
            double elapsed_s = (DateTime.Now - start).TotalMilliseconds/1000.0;
            Console.WriteLine("search for [{0}] returned {1} hits in {2}s   ({3} productions, {4} comparisons)", 
                expression, hits.Count, elapsed_s, stats.unique_hits_produced, stats.comparisons);
            Console.WriteLine("    " + String.Join(",",hits.Count < 15 ? hits : hits.GetRange(0,15)));
            
        }

        public void find_email_test() {

            System.Console.WriteLine("### In email test");

            System.Console.WriteLine("### term intersection ");

            searchFor("jeske neotonic");
            searchFor("noticed problems");
            searchFor("data returned");
            searchFor("scott hassan");
        }

    }


}
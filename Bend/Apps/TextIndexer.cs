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

        public struct TermHit {
            public string word;
            public string docid;
            public string position;

            public override string ToString() {
                return String.Format("{0} at ({1} : {2})", word, docid, position);
            }
        }

        public class TermHits {
            string word;           
            TextIndexer index;
            public TermHits(TextIndexer indexer, string word) {
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
                TermHit hit;
                hit.word = hitrow.key_parts[len_of_index_prefix+0];
                hit.docid = hitrow.key_parts[len_of_index_prefix + 1];
                hit.position = hitrow.key_parts[len_of_index_prefix + 2];
                return hit;
            }

            public TermHit advanceTo(TermHit newpos) {
                //    ".zindex/index/<word>/<docid>"
                var keysearch = new RecordKey().appendParsedKey(this.index.index_location_prefix)
                    .appendKeyPart(this.word).appendKeyPart(newpos.docid).appendKeyPart(newpos.position);

                KeyValuePair<RecordKey, RecordData> row = index.db.FindNext(keysearch, false);
                TermHit hit = unpackHit(row.Key);
                if (hit.word.CompareTo(this.word) == 0) {
                    return hit;
                } else {
                    throw new KeyNotFoundException(
                        String.Format("advanceTo({0}): no more hits for {1}", newpos, this.word));
                }
            }
        }


        public void index_document(string docid, string txtbody) {
            //System.Console.WriteLine(msg.Body);
            int wordpos = 0;

            foreach (var possibleword in Regex.Split(txtbody, @"[-*()\""'[\]:\s?.,]+")) {
                String srcword = possibleword;
                srcword = Regex.Replace(srcword,@"([-""':+_=\/|]{3,})","");

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
                this.db.setValue(key, RecordUpdate.WithPayload(""));
                wordpos++;
            }

        }


        public void find_email_test() {
            string[] words_to_find = { "you", "about" };

            System.Console.WriteLine("### In email test");
            var hit_walkers = new List<TermHits>();
            foreach (var word in words_to_find) {

                var hits = new TermHits(this, word);
                hit_walkers.Add(hits);

                System.Console.WriteLine("** occuraces of term: {0}", word);
                foreach (var occurance in hits.allOccurances()) {                    
                    System.Console.WriteLine(occurance);
                }
            }

            System.Console.WriteLine("### term intersection ");

            var term1 = new TermHits(this, "you");
            var term2 = new TermHits(this, "about");

            TermHit hit;
            hit.docid = "";
            hit.position = "";
            hit.word = "";

            TermHit hit1 = term1.advanceTo(hit);
            TermHit hit2 = term2.advanceTo(hit);
            int count = 0;

            try {
                while (true) {
                    switch (hit1.docid.CompareTo(hit2.docid)) {
                        case -1:
                            System.Console.WriteLine("     advance1: {0} == {1}", hit1, hit2);
                            hit1 = term1.advanceTo(hit2);
                            break;
                        case 1:
                            System.Console.WriteLine("     advance2: {0} == {1}", hit1, hit2);
                            hit2 = term2.advanceTo(hit1);
                            break;
                        case 0:
                            System.Console.WriteLine("match: {0} == {1}", hit1, hit2);
                            hit1 = term1.advanceTo(hit1);
                            hit2 = term2.advanceTo(hit2);
                            break;
                    }
                    if (count++ > 40) {
                        Console.WriteLine("dumping out");
                        return;
                    }
                }
            } catch (KeyNotFoundException) {
                // done finding hits
            }


        }

    }


}
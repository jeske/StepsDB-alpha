// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;



namespace Bend
{
    public class PipeGenerateException : Exception
    {
        PipeStage stage;
        string msg;
        public PipeGenerateException(PipeStage stage, string msg) {
            this.stage = stage;
            this.msg = msg;
        }
        public override string ToString() {
            return "" + this.stage.ToString() + " : " + this.msg;
        }
    }
    // ----------------------------------[   qualifiers ]-------------------------------

    public enum QualifierResult
    {
        DESIRE_LT,    // we desire a key less than the current key
        DESIRE_GT,    // we desire a key greater than the current key
        MATCH,        // the current key matches
        NOMATCH       // the current key does not match, and this operator is nor orderable
    }

    public enum QualifierSetupResult
    {
        SETUP_OK,           // we are setup to watch for the next key in a scan
        NO_MORE_MATCHES     // there are no more matching keys in this direction
    }

    public abstract class QualifierBase 
    {
        public abstract QualifierResult KeyCompare(string key);
        public abstract QualifierSetupResult setupForNext(string key); // return false if the next key is invalid
        public abstract QualifierSetupResult setupForPrev(string key); // return false if the next key is 
    }

    public class QualifierException : Exception
    {
        public QualifierException(string msg) : base(msg) { }
    }

    public sealed class QualifierExact : QualifierBase
    {
        string value;
        int exact_hash_delta;
        public QualifierExact(string value) { 
            this.value = value;
            if (value == null) { 
                throw new QualifierException("QualifierExact may not be null"); 
            }
            exact_hash_delta = "EXACT".GetHashCode();
        }
        public override string ToString() {
            return "=" + value;
        }
        public override QualifierResult KeyCompare(string keydata) {
            if (keydata == null) {
                throw new QualifierException("QualifierExact.KeyCompare(null) is invalid");
            }
            int compare_result = value.CompareTo(keydata);
            if (compare_result == 0) {  // equals
                return QualifierResult.MATCH;
            } else if (compare_result < 0) { //  QUAL_TARGET < keydata
                return QualifierResult.DESIRE_LT;
            } else {  // QUAL_TARGET > keydata
                return QualifierResult.DESIRE_GT;
            }
        }
        public override QualifierSetupResult setupForNext(string keydata) {
            if (keydata == null) {
                throw new QualifierException("QualifierExact.setupForNext(null) is invalid");
            }
            if (this.value == keydata) {
                return QualifierSetupResult.SETUP_OK;
            } else {
                return QualifierSetupResult.NO_MORE_MATCHES;
            }
        }
        public override QualifierSetupResult setupForPrev(string keydata) {
            if (keydata == null) { 
                throw new QualifierException("QualifierExact.setupForPrev(null) is invalid"); 
            }
            if (this.value == keydata) {
                return QualifierSetupResult.SETUP_OK;
            } else {
                return QualifierSetupResult.NO_MORE_MATCHES;
            }
        }
        public override bool Equals(object obj) {
            if (obj.GetType() != typeof(QualifierExact)) {
                return false;
            } else {
                QualifierExact obj_exact = (QualifierExact)obj;
                return this.value.Equals(obj_exact.value);
            }
        }
        public override int GetHashCode() {
            return this.exact_hash_delta + value.GetHashCode();
        }
    }

    // ----------------------------------[    PipeHdfContext   ]-------------------------

    public class PipeHdfContext {
        private Dictionary<string,QualifierBase> fields;
        private Dictionary<string,PipeHdfContext> subtrees;
        public PipeHdfContext() {
            fields = new Dictionary<string,QualifierBase>();
        }
        // ---------
    
        public void setQualifier(string field,QualifierBase qualifier) {
            fields[field] = qualifier;
        }
        public void setQualifier(string field, string exact_value) {
            this.setQualifier(field, new QualifierExact(exact_value));
        }
        public QualifierBase getQualifier(string field,QualifierBase default_qualifier) {
            try {
                return fields[field];
            } catch (KeyNotFoundException) {
                return default_qualifier;
            }
        }
        public QualifierBase getQualifier(string field, string default_exact_value) {
              return getQualifier(field,new QualifierExact(default_exact_value));
        }
        public QualifierBase getQualifier(string field) {
            return getQualifier(field, (QualifierBase)null);
        }
        public PipeHdfContext getSubtree(string key) {
            if (subtrees != null) {
                return subtrees[key];
            } else {
                return null;
            }
        }
        public void setSubtree(string key, PipeHdfContext subtree) {
            if (subtrees == null) {
                subtrees = new Dictionary<string, PipeHdfContext>();
            }
            subtrees[key] = subtree;
        }
    }

    //  -----------------[ PipeRowBuilder / PipeRow ]--------------------------------

    public class PipeRowBuilder {
        List<QualifierBase> key_part_qualifiers;
        List<string> data_parts;
        public PipeRowBuilder() {
            key_part_qualifiers = new List<QualifierBase>();
            data_parts = new List<string>();
        }
        public PipeRowBuilder appendKeyPart(QualifierBase qualifier) {
            key_part_qualifiers.Add(qualifier);
            return this;
        }

        // we need STATIC  Qualifiers: exact, prefix, range, exclusion, contains
        // we need DYNAMIC Qualifiers: callback (for joins, for example)

        // TODO: 
        //
        // try to design a way for us to assemble a MAPPING from the hdf-context to keys and data,
        // so we can use these pipe elements to evaluate HDF context states, instead of key and data
        // parts. That way, we can translate a tree without relying on the pipes. 
        //
        // This will also allow us to make "qualifiers" generic, and apply on both keys and data. 
        //
        // For example, the table manager would one-time suppily a mapping evaluator for
        // the table-materialization metadata, which we would read directly, instead of
        // relying on the table manager to remain "correct". 

        public string ToString() {
            string output = "";
            foreach (QualifierBase part in key_part_qualifiers) {
                output = output + "/" + part.ToString();
            }
            return output;
        }
    }

    public class PipeRow {
    }

    // ---------------------------------[ PipeStage ]-----------------------------------------

    public abstract class PipeStage {
        internal abstract void _generateRowFromContext(PipeHdfContext ctx, PipeRowBuilder rowb);
        public PipeRowBuilder generateRowFromContext(PipeHdfContext ctx) {
            // generate our own rowparts
            PipeRowBuilder builder = new PipeRowBuilder();
            this._generateRowFromContext(ctx, builder);
            return builder;
        }
        //------
        internal abstract void _generateContextFromRow(PipeRow row, PipeHdfContext ctx);
        public PipeHdfContext generateContextFromRow(PipeRow row) {
            PipeHdfContext ctx = new PipeHdfContext();
            this._generateContextFromRow(row, ctx);
            return ctx;
        }
    }

    // -----[ PipeStageEnd  ]-----
    public class PipeStageEnd : PipeStage
    {
        public PipeStageEnd() { }
        internal override void _generateRowFromContext(PipeHdfContext ctx, PipeRowBuilder rob) { }
        internal override void _generateContextFromRow(PipeRow row, PipeHdfContext ctr) {  }
    }

    // -----[ PipeStagePartition  ]-----
    class PipeStagePartition : PipeStage {
        PipeStage next_stage;
        internal string context_key;  // we should really make this strong-named, not just a string
        public PipeStagePartition(string context_key, PipeStage next_stage) {
            this.context_key = context_key;
            this.next_stage = next_stage;
        }
        internal override void _generateRowFromContext(PipeHdfContext ctx, PipeRowBuilder rob) {
            QualifierBase qual = ctx.getQualifier(this.context_key);
            if (qual != null) {
                rob.appendKeyPart(qual);
            } else {
                throw new PipeGenerateException(this, "missing context: " + this.context_key);
            }
            this.next_stage._generateRowFromContext(ctx, rob);
        }
        internal override void _generateContextFromRow(PipeRow row, PipeHdfContext ctr) {
            // TODO
        }
    }

    // -----[ PipeStagePartitionDeferred  ]-----
    /*
    class PipeStagePartitionDeferred : PipeStage {
    }
     */

    // -----[ PipeStageMux  ]-----
    public class PipeStageMux : PipeStage {
        Dictionary<QualifierExact, PipeStage> mux_map;
        string context_key;
        QualifierExact default_muxvalue;

        public PipeStageMux(
            string context_key, 
            Dictionary<QualifierExact, PipeStage> mux_map, 
            QualifierExact default_muxvalue
            ) {
            this.mux_map = new Dictionary<QualifierExact, PipeStage>(mux_map);
            this.context_key = context_key;
            this.default_muxvalue = default_muxvalue;
        }
        public PipeStageMux(string context_key, Dictionary<QualifierExact, PipeStage> mux_map) 
            : this(context_key, mux_map, null) { }

        internal override void _generateRowFromContext(PipeHdfContext ctx, PipeRowBuilder rob) {
            // TODO typecheck this cast and throw a useful execption
            QualifierExact mux_key = (QualifierExact) ctx.getQualifier(this.context_key, this.default_muxvalue);

            if (mux_key != null) {
                rob.appendKeyPart(mux_key);
                {
                    PipeStage next_stage;
                    try {
                        next_stage = this.mux_map[mux_key];
                    }
                    catch (KeyNotFoundException) {
                        throw new PipeGenerateException(this, "missing mux_map entry for: " +
                            this.context_key + " : " + mux_key);
                    }
                    next_stage._generateRowFromContext(ctx, rob);
                }
            } else {
                throw new PipeGenerateException(this, "missing mux context: " + this.context_key);
            }
        }
        internal override void _generateContextFromRow(PipeRow row, PipeHdfContext ctr) {
        }
        
    }
    

}

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
    }

    // ----------------------------------[    PipeHdfContext   ]-------------------------

    public class PipeHdfContext {
        private Dictionary<string,string> keys;
        private Dictionary<string,PipeHdfContext> subtrees;
        public PipeHdfContext() {
            keys = new Dictionary<string,string>();
        }
        // ---------
    
        public void setValue(string key,string value) {
            keys[key] = value;
        }

        public string getValue(string key,string default_value) {
            try {
                return keys[key];
            } catch (KeyNotFoundException) {
                return default_value;
            }
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
        List<string> key_parts;
        List<string> data_parts;
        public PipeRowBuilder() {
            key_parts = new List<string>();
            data_parts = new List<string>();
        }
        public PipeRowBuilder appendKeyPartQualifierExact(string keypart) {
            key_parts.Add(keypart);
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
            return "/" + String.Join(new String('/', 1), key_parts.ToArray());
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
            string value = ctx.getValue(this.context_key, null);
            if (value != null) {
                rob.appendKeyPartQualifierExact(value);
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
        Dictionary<string, PipeStage> mux_map;
        string context_key;
        string default_muxvalue;

        public PipeStageMux(string context_key, Dictionary<string, PipeStage> mux_map, string default_muxvalue) {
            this.mux_map = new Dictionary<String, PipeStage>(mux_map);
            this.context_key = context_key;
            this.default_muxvalue = default_muxvalue;
        }
        public PipeStageMux(string context_key, Dictionary<string, PipeStage> mux_map) 
            : this(context_key, mux_map, null) { }

        internal override void _generateRowFromContext(PipeHdfContext ctx, PipeRowBuilder rob) {
            string mux_key = ctx.getValue(this.context_key, this.default_muxvalue);
            if (mux_key != null) {
                rob.appendKeyPartQualifierExact(mux_key);
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

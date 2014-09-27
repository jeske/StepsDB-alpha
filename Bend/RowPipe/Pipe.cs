// Copyright (C) 2008-2014 David W. Jeske
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.


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

    //  -----------------[ PipeRowQualifier / PipeRow ]--------------------------------

    public class PipeRowQualifier {
        List<QualifierBase> key_part_qualifiers;
        List<string> data_parts;
        public PipeRowQualifier() {
            key_part_qualifiers = new List<QualifierBase>();
            data_parts = new List<string>();
        }
        public PipeRowQualifier appendKeyPart(QualifierBase qualifier) {
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

        public IEnumerator<QualifierBase> GetEnumerator() {
            return key_part_qualifiers.GetEnumerator();
        }

        public override string ToString() {
            string output = "";
            foreach (QualifierBase part in key_part_qualifiers) {
                output = output + "/" + part.ToString();
            }
            return output;
        }

        public RecordKey genLowestKey() {
            RecordKey key = new RecordKey();
            foreach (QualifierBase part in key_part_qualifiers) {
                key.appendKeyPart(part.genLowestKeyTest().ToString());
            }
            return key;
        }
        public RecordKey genHighestKey() {
            RecordKey key = new RecordKey();
            foreach (QualifierBase part in key_part_qualifiers) {
                key.appendKeyPart(part.genHighestKeyTest().ToString());
            }
            return key;


        }

    }

    public class PipeRow {
    }

    // ---------------------------------[ PipeStage ]-----------------------------------------

    public abstract class PipeStage {
        internal abstract void _generateRowQualifierFromContext(PipeHdfContext ctx, PipeRowQualifier rowb);
        public PipeRowQualifier generateRowFromContext(PipeHdfContext ctx) {
            // generate our own rowparts
            PipeRowQualifier builder = new PipeRowQualifier();
            this._generateRowQualifierFromContext(ctx, builder);
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
        internal override void _generateRowQualifierFromContext(PipeHdfContext ctx, PipeRowQualifier rob) { }
        internal override void _generateContextFromRow(PipeRow row, PipeHdfContext ctr) {  }
    }

    // -----[ PipeStagePartition  ]-----
    public class PipeStagePartition : PipeStage {
        PipeStage next_stage;
        internal string context_key;  // we should really make this strong-named, not just a string
        public PipeStagePartition(string context_key, PipeStage next_stage) {
            this.context_key = context_key;
            this.next_stage = next_stage;
        }
        internal override void _generateRowQualifierFromContext(PipeHdfContext ctx, PipeRowQualifier rob) {
            QualifierBase qual = ctx.getQualifier(this.context_key);
            if (qual != null) {
                rob.appendKeyPart(qual);
            } else {
                throw new PipeGenerateException(this, "missing context: " + this.context_key);
            }
            this.next_stage._generateRowQualifierFromContext(ctx, rob);
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

        internal override void _generateRowQualifierFromContext(PipeHdfContext ctx, PipeRowQualifier rob) {
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
                    next_stage._generateRowQualifierFromContext(ctx, rob);
                }
            } else {
                throw new PipeGenerateException(this, "missing mux context: " + this.context_key);
            }
        }
        internal override void _generateContextFromRow(PipeRow row, PipeHdfContext ctr) {
        }
        
    }
    

}

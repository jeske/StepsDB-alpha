// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A01_PipeTests
    {

        [Test]
        public void T01_PipeHdfContext() {
            PipeHdfContext ctx2 = new PipeHdfContext();
            ctx2.setQualifier("blah", "1");


            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setQualifier("foo", "bar");
            ctx.setQualifier("baz", "blah");
            ctx.setSubtree("baz", ctx2);


            Assert.AreEqual(new QualifierExact("bar"), ctx.getQualifier("foo", "deffoo"));
            Assert.AreEqual(new QualifierExact("blah"), ctx.getQualifier("baz", "defblah"));
            Assert.AreEqual(new QualifierExact("def not present"), ctx.getQualifier("notpresent", "def not present"));

            Assert.AreEqual(ctx2, ctx.getSubtree("baz"));
        }

        [Test]
        public void T03_PipeStages() {
            // setup the pipe...
            PipeStagePartition p =
                new PipeStagePartition("username",
                    new PipeStagePartition("tablename",
                        new PipeStageEnd()
                    )
                );

            // setup the HDF context
            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setQualifier("username", "jeske");

            {
                bool err = false;
                // process the HDF context, should be an error because "table" doesn't exist..
                try {
                    p.generateRowFromContext(ctx);
                } catch (PipeGenerateException) {
                    err = true;                    
                }
                Assert.AreEqual(true, err, "error on pipe element not in context");
            }

            // add the "tablename" key
            ctx.setQualifier("tablename", "foo");

            // process the HDF context, should be an error because "table" doesn't exist..
            PipeRowQualifier newrow = p.generateRowFromContext(ctx);

            // test the key produced
            Assert.AreEqual("/=jeske/=foo", newrow.ToString());

        }

        [Test]
        public void T04_PipeMux() {
            // setup the pipe...
            PipeStagePartition p =
                new PipeStagePartition("username",
                    new PipeStagePartition("table",
                        new PipeStageEnd()
                    )
                );

            Dictionary<QualifierExact, PipeStage> mux_map = new Dictionary<QualifierExact, PipeStage>();
            mux_map.Add(new QualifierExact("path_a"), 
                new PipeStagePartition("suba", new PipeStageEnd()));
            mux_map.Add(new QualifierExact("path_b"), 
                new PipeStagePartition("subb", new PipeStageEnd()));

            PipeStageMux mux = new PipeStageMux("select_sub", mux_map);

            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setQualifier("suba", "suba-keypart");
            ctx.setQualifier("subb", "subb-keypart");
            // test missing muxkey with no default
            {
                bool err = false;
                try {
                    PipeRowQualifier builder = mux.generateRowFromContext(ctx);
                }
                catch (PipeGenerateException) {
                    err = true;
                }
                Assert.AreEqual(err, true, "non-default muxkey should throw exception");
            }

            // test mux defaults
            {
                PipeStageMux defmux = new PipeStageMux("select_sub", mux_map, new QualifierExact("path_a"));
                PipeRowQualifier builder = defmux.generateRowFromContext(ctx);
                Assert.AreEqual("/=path_a/=suba-keypart", builder.ToString());
            }


            // test suba
            {
                ctx.setQualifier("select_sub", "path_a");
                PipeRowQualifier builder = mux.generateRowFromContext(ctx);
                Assert.AreEqual("/=path_a/=suba-keypart", builder.ToString());
            }

            // test subb
            {
                ctx.setQualifier("select_sub", "path_b");
                PipeRowQualifier builder = mux.generateRowFromContext(ctx);
                Assert.AreEqual("/=path_b/=subb-keypart", builder.ToString());
            }

        }

    }





}
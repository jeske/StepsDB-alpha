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
        public void T00_PipeHdfContext() {
            PipeHdfContext ctx2 = new PipeHdfContext();
            ctx2.setValue("blah", "1");


            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setValue("foo", "bar");
            ctx.setValue("baz", "blah");
            ctx.setSubtree("baz", ctx2);


            Assert.AreEqual("bar", ctx.getValue("foo", "deffoo"));
            Assert.AreEqual("blah", ctx.getValue("baz", "defblah"));
            Assert.AreEqual("def not present", ctx.getValue("notpresent", "def not present"));

            Assert.AreEqual(ctx2, ctx.getSubtree("baz"));
        }

        [Test]
        public void T02_PipeBuilder() {
            PipeRowBuilder builder = new PipeRowBuilder();
            builder.appendKeyPartQualifierExact("test");
            builder.appendKeyPartQualifierExact("blah");

            Assert.AreEqual("/test/blah", builder.ToString());

        }

        [Test]
        public void T03_PipePartition() {
            // setup the pipe...
            PipeStagePartition p =
                new PipeStagePartition("username",
                    new PipeStagePartition("table",
                        new PipeStageEnd()
                    )
                );

            // setup the HDF context
            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setValue("username", "jeske");

            {
                bool err = false;
                // process the HDF context, should be an error because "table" doesn't exist..
                try {
                    p.generateRowFromContext(ctx);
                }
                catch (PipeGenerateException exc) {
                    err = true;
                }
                Assert.AreEqual(true, err, "error on pipe element not in context");
            }

            // add the "table" key
            ctx.setValue("table", "foo");

            // process the HDF context, should be an error because "table" doesn't exist..
            PipeRowBuilder newrow = p.generateRowFromContext(ctx);

            // test the key produced
            Assert.AreEqual("/jeske/foo", newrow.ToString());

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

            Dictionary<string, PipeStage> mux_map = new Dictionary<string, PipeStage>();
            mux_map.Add("path_a", new PipeStagePartition("suba", new PipeStageEnd()));
            mux_map.Add("path_b", new PipeStagePartition("subb", new PipeStageEnd()));

            PipeStageMux mux = new PipeStageMux("select_sub", mux_map);

            PipeHdfContext ctx = new PipeHdfContext();
            ctx.setValue("suba", "suba-keypart");
            ctx.setValue("subb", "subb-keypart");
            // test missing muxkey with no default
            {
                bool err = false;
                try {
                    PipeRowBuilder builder = mux.generateRowFromContext(ctx);
                }
                catch (PipeGenerateException) {
                    err = true;
                }
                Assert.AreEqual(err, true, "non-default muxkey should throw exception");
            }

            // test mux defaults
            {
                PipeStageMux defmux = new PipeStageMux("select_sub", mux_map, "path_a");
                PipeRowBuilder builder = defmux.generateRowFromContext(ctx);
                Assert.AreEqual("/path_a/suba-keypart", builder.ToString());
            }


            // test suba
            {
                ctx.setValue("select_sub", "path_a");
                PipeRowBuilder builder = mux.generateRowFromContext(ctx);
                Assert.AreEqual("/path_a/suba-keypart", builder.ToString());
            }

            // test subb
            {
                ctx.setValue("select_sub", "path_b");
                PipeRowBuilder builder = mux.generateRowFromContext(ctx);
                Assert.AreEqual("/path_b/subb-keypart", builder.ToString());
            }

        }

    }





}
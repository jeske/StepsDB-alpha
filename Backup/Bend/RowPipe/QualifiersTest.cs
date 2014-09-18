// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.Collections.Generic;

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A00_QualifierTests
    {
        [Test]
        public void T00_QualifierAny() {
            QualifierAny qa = new QualifierAny();

        }
        [Test]
        public void T00_QualifierExact() {
            QualifierExact qe = new QualifierExact("M");

            // test range start/end/key generation

            Assert.AreEqual("=M", qe.genLowestKeyTest());
            Assert.AreEqual("=M", qe.genHighestKeyTest());
            Assert.AreEqual("=M", qe.ToString());

            // test basic key match
            Assert.AreEqual(false, qe.MatchTo(new QualifierExact("A").ToString()));
            Assert.AreEqual(false, qe.MatchTo(new QualifierExact("ZZ").ToString()));
            Assert.AreEqual(true, qe.MatchTo(new QualifierExact("M").ToString()));
            
            // test exceptions
            // Assert.Fail("need to test exceptions");
        }

        [Test]
        public void T02_PipeRowQualifier() {
            PipeRowQualifier builder = new PipeRowQualifier();
            builder.appendKeyPart(new QualifierExact("test"));
            builder.appendKeyPart(new QualifierExact("blah"));

            Assert.AreEqual("/=test/=blah", builder.ToString());

        }
    }
}
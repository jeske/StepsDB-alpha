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
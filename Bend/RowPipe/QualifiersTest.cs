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

            // test basic key match
            Assert.AreEqual(QualifierResult.DESIRE_GT, qe.KeyCompare(""));
            Assert.AreEqual(QualifierResult.DESIRE_LT, qe.KeyCompare("Z"));
            Assert.AreEqual(QualifierResult.DESIRE_GT, qe.KeyCompare("A"));
            Assert.AreEqual(QualifierResult.MATCH, qe.KeyCompare("M"));

            // test range scan
            Assert.AreEqual(QualifierSetupResult.SETUP_OK, qe.setupForNext("M"));
            Assert.AreEqual(QualifierSetupResult.SETUP_OK, qe.setupForPrev("M"));
            Assert.AreEqual(QualifierSetupResult.NO_MORE_MATCHES, qe.setupForNext("ZZ"));
            Assert.AreEqual(QualifierSetupResult.NO_MORE_MATCHES, qe.setupForPrev("ZZ"));
            Assert.AreEqual(QualifierSetupResult.NO_MORE_MATCHES, qe.setupForNext("AA"));
            Assert.AreEqual(QualifierSetupResult.NO_MORE_MATCHES, qe.setupForPrev("AA"));


            // test null value throws QualifierException
            {
                bool err = false;
                try {
                    qe.KeyCompare(null);
                }
                catch (QualifierException) {
                    err = true;
                }
                Assert.AreEqual(true, err, "QualiferExact.KeyCompare(null) should throw QualifierException");

                err = false;
                try {
                    qe.setupForNext(null);
                }
                catch (QualifierException) {
                    err = true;
                }
                Assert.AreEqual(true, err, "QualifierExact.setupForNext(null) should throw QualifierException");


                err = false;
                try {
                    qe.setupForPrev(null);
                }
                catch (QualifierException) {
                    err = true;
                }
                Assert.AreEqual(true, err, "QualifierExact.setupForPrev(null) should throw QualifierException");
            }


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
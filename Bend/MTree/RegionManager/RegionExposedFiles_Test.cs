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
using System.IO;

using System.Threading;
using System.Collections;
using System.Collections.Generic;

// TODO: raw file, raw partition region managers, tests



namespace BendTests {
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A01_RegionExposedFiles {
        // TODO: make a basic region test
        long BLOCK_SIZE = 4 * 1024 * 1024;

        [Test]
        public void T05_Region_Concurrency() {
            RegionExposedFiles rm = new RegionExposedFiles(InitMode.NEW_REGION,
                    "C:\\BENDtst\\T05_Region_Concurrency");
            byte[] data = { 1, 3, 4, 5, 6, 7, 8, 9, 10 };

            {
                // put some data in the region
                IRegion region1 = rm.writeFreshRegionAddr(0, BLOCK_SIZE);
                {
                    Stream output = region1.getNewAccessStream();
                    output.Write(data, 0, data.Length);
                    output.Close();
                }
            }

            {
                IRegion region1 = rm.readRegionAddrNonExcl(0);
                Stream rd1 = region1.getNewAccessStream();


                Stream rd2 = region1.getNewAccessStream();

                // Assert.AreNotEqual(rd1, rd2, "streams should be separate");

                for (int i = 0; i < data.Length; i++) {
                    Assert.AreEqual(0, rd2.Position, "stream rd2 position should be indpendent");

                    Assert.AreEqual(i, rd1.Position, "stream rd1 position");
                    Assert.AreEqual(data[i], rd1.ReadByte(), "stream rd1 data correcness");
                }

            }


        }


        [Test]
        public void T02_Region_References() {
            RegionExposedFiles rm = new RegionExposedFiles(InitMode.NEW_REGION,
                    "C:\\BENDtst\\T01_Region_References");

            byte[] data = { 1, 3, 4, 5, 6, 7, 8, 9, 10 };
            bool delegate_called = false;

            {
                // put some data in the region
                IRegion region1 = rm.writeFreshRegionAddr(0, BLOCK_SIZE);
                {
                    Stream output = region1.getNewAccessStream();
                    output.Write(data, 0, data.Length);
                    output.Dispose();
                }
                region1 = null;
            }
            System.GC.Collect();

            {
                IRegion region2 = rm.readRegionAddrNonExcl(0);
                Stream rd1 = region2.getNewAccessStream();
                rm.notifyRegionSafeToFree(0, delegate(long addr) {
                    System.Console.WriteLine("** region safe to free delegate called");
                    delegate_called = true; 
                });
                rd1 = null;
                region2 = null;
            }
            rm = null;

            for (int x = 0; x < 1000; x++) {
                Thread.Sleep(5);
                System.GC.Collect();                
                if (delegate_called) { break; }
            }
            Assert.AreEqual(true, delegate_called, "region was never safe to free");
        }


        class Foo {
            public Foo() {
                System.Console.WriteLine("foo allocated");
            }
            ~Foo() {
                System.Console.WriteLine("Finalize Called on Foo");
                GC.SuppressFinalize(this);
            }
        }
        [Test]
        public void T01_TestDispose() {
            {
                Foo f = new Foo();
                f = null;
            }

            for (int x = 0; x < 1000; x++) {
                GC.Collect(0);
                Thread.Sleep(5);
            }


        }
    }

}

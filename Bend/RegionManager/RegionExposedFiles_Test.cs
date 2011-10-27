

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
                    output.Dispose();
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
    }

}

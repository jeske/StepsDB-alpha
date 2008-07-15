// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.

using System;
using System.IO;
using System.Collections.Generic;

using NUnit.Framework;
using Bend;

namespace BendPerfTest
{
    using System.Threading;
    using BendTests;
    
    public partial class A01_Block_Perf
    {

        class CompressedBlockTestFactory : IBlockTestFactory
        {

            public ISegmentBlockEncoder makeEncoder() {
                return new SegmentBlockCompressedEncoder(new SegmentBlockBasicEncoder());
            }

            public ISegmentBlockDecoder makeDecoder(BlockAccessor block) {
                return new SegmentBlockBasicDecoder(
                    SegmentBlockCompressedDecodeStage.decode(block));
            }
        }

        [Test]
        public void T10_CompressedBlock_Perftest() {
            SegmentBlock_Tests.Block_Perftest(new CompressedBlockTestFactory());
        } // testend
    }
}

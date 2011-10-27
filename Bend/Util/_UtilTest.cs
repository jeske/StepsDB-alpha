

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public partial class A00_UtilTest
    {

        [Test]
        public void T01_FastUniqueIds() {
            FastUniqueIds idgen = new FastUniqueIds();

            for (int x = 0; x < 1000; x++) {

                long t1 = idgen.nextTimestamp();
                long t2 = idgen.nextTimestamp();

                Assert.AreNotEqual(t1, t2, "unique ids collide");
            }
        }

    }


}
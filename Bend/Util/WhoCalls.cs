
using System;
using System.Diagnostics;
using System.Reflection;


namespace Bend {
    public class WhoCalls
    {
        public static string WhatsMyName()
        {
            StackTrace stackTrace = new StackTrace();
            StackFrame stackFrame = stackTrace.GetFrame(1);
            MethodBase methodBase = stackFrame.GetMethod();

            return methodBase.Name;
        }
        public static string WhoCalledMe()
        {
            StackTrace stackTrace = new StackTrace();
            StackFrame stackFrame = stackTrace.GetFrame(2);
            MethodBase methodBase = stackFrame.GetMethod();
            
            return methodBase.Name;
        }


    }
}



namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class TestWhoCalls
    {

        public string TestWhoCalledMe()
        {
            return WhoCalls.WhoCalledMe();
        }
        [Test]
        public void T00_WhoCalls()
        {
            Assert.AreEqual("T00_WhoCalls", WhoCalls.WhatsMyName());
            Assert.AreEqual("T00_WhoCalls", TestWhoCalledMe());
        }
    }
}
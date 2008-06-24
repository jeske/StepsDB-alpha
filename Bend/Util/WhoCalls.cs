
// public code from:
// http://www.codeproject.com/KB/dotnet/MethodName.aspx

using System;
using System.Diagnostics;
using System.Reflection;


namespace Bend {
    public class WhoCalls
    {
        private static void WhatsMyName()
        {
            StackFrame stackFrame = new StackFrame();
            MethodBase methodBase = stackFrame.GetMethod();
            Console.WriteLine(methodBase.Name); // Displays “WhatsmyName”

            WhoCalledMe();
        }
        // Function to display parent function

        private static void WhoCalledMe()
        {
            StackTrace stackTrace = new StackTrace();
            StackFrame stackFrame = stackTrace.GetFrame(1);
            MethodBase methodBase = stackFrame.GetMethod();
            // Displays “WhatsmyName”

            Console.WriteLine(" Parent Method Name {0} ", methodBase.Name);
        }

    }
}
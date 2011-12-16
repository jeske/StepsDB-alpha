// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.IO;
using System.Reflection;

// This class makes it easy to deal with serialization and deserialization by using reflection....
//
// HOWEVER, MSDN says:
//
// The GetFields method does not return fields in a particular order, such as alphabetical or 
// declaration order. Your code must not depend on the order in which fields are returned, because 
// that order varies.
//
// Which mwans depending on this code is bad, and is violating some possibility in .NET design.
//
// http://msdn.microsoft.com/en-us/library/6ztex2dc.aspx
//
//
// ALSO, this implementation does not see internal members.

namespace Bend {

    public static partial class Util
    {
        // how do we copy from struct to the Stream ? 
        // all the solutions seem bad...
        //   http://www.codeproject.com/KB/cs/C__Poiter.aspx
        //   http://dotnetjunkies.com/WebLog/chris.taylor/articles/9016.aspx
        // 
        // here is a better solution using reflection..


        // http://www.megasolutions.net/cSharp/Using-reflection-to-show-all-fields-values-of-a-struct_-69838.aspx
        public static void writeStruct<E>(E obj, BinaryWriter w) where E : struct {
            Type otype = obj.GetType();
            // MemberInfo[] members = otype.GetMembers();
            FieldInfo[] fields = otype.GetFields();
            foreach (FieldInfo f in fields) {
                Type t = f.FieldType;
                if (t == typeof(System.UInt32)) {
                    w.Write((System.UInt32)f.GetValue(obj));
                } else if (t == typeof(System.Int32)) {
                    w.Write((System.Int32)f.GetValue(obj));
                } else if (t == typeof(System.Int64)) {
                    w.Write((System.Int64)f.GetValue(obj));
                } else {
                    throw new Exception("BinStruct unimplemented type: " + t.ToString());
                }
            }
        }
            
        public static void writeStruct<E>(E obj, Stream output) where E : struct{
            BinaryWriter w = new BinaryWriter(output);
            writeStruct<E>(obj, w);
            
        }

        public static void writeStruct<E>(E obj, out byte[] outbuf) where E : struct {
            MemoryStream ms = new MemoryStream();
            writeStruct(obj, ms);
            outbuf = ms.ToArray();
        }



        // http://mmarinov.blogspot.com/2007/01/reflection-modify-value-types-by.html
        public static E  readStruct<E>(BinaryReader r) where E: struct {
            
            E val = new E();

            // TypedReference vref = __makeref(val);
            Object oval = (Object)val;

            Type vtype = typeof(E);
            FieldInfo[] fields = vtype.GetFields();
            foreach (FieldInfo f in fields) {
                Type t = f.FieldType;
                if (t == typeof(System.UInt32)) {
                    // f.SetValueDirect(vref, r.ReadUInt32());
                    f.SetValue(oval, r.ReadUInt32());
                } else if (t == typeof(System.Int32)) {
                    // f.SetValueDirect(vref, r.ReadInt32());                    
                    f.SetValue(oval, r.ReadInt32());
                } else if (t == typeof(System.Int64)) {
                    f.SetValue(oval, r.ReadInt64());
                } else {
                    throw new Exception("BinStruct unimplemented type: " + t.ToString());
                }
            }
            // return val;
            return (E)oval;
        }

        public static E readStruct<E>(Stream input) where E : struct {
            BinaryReader r = new BinaryReader(input);
            return readStruct<E>(r);
        }

        public static int structSize<T>(ref T stobj) where T : struct {
                int size = 0;
                Type otype = stobj.GetType();
                // MemberInfo[] members = otype.GetMembers();
                
                FieldInfo[] fields = otype.GetFields();
                foreach (FieldInfo f in fields) {
                    Type t = f.FieldType;
                    if (t == typeof(System.UInt32)) {                        
                        size += sizeof(System.UInt32);
                    } else if (t == typeof(System.Int32)) {
                        size += sizeof(System.Int32);
                    } else if (t == typeof(System.Int64)) {
                        size += sizeof(System.Int64);
                    } else {
                        throw new Exception("BinStruct unimplemented type: " + t.ToString());
                    }
                }
            return size;
        }


    }

}

namespace BendTests {
    using Bend;
    using NUnit.Framework;

    public partial class A00_UtilTest
    {
        struct Test
        {
            public uint a;
            public uint b;
        }

        [Test]
        public void T00_structWriteRead() {
            // test struct write/read
            Test st;
            st.a = 1;
            st.b = 2;
            MemoryStream stout = new MemoryStream();
            Util.writeStruct(st, stout);
            byte[] data = stout.ToArray();

            // dump the struct
            if (true) {
                System.Console.WriteLine("struct conversion size: " + data.Length);
                System.Console.WriteLine("contents: ");
                foreach (byte b in data) {
                    System.Console.Write((int)b);
                    System.Console.Write(" ");
                }
            }

            Test st2;
            MemoryStream msin = new MemoryStream(data);
            st2 = Util.readStruct<Test>(msin);

            Assert.AreEqual(msin.Position, msin.Length, "struct conversion didn't consume entire buffer");
            Assert.AreEqual(st, st2, "struct conversion test failed");    
        
        }
    }



}
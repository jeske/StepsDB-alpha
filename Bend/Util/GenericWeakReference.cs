// this code used from CodeProject
// http://69.10.233.10/KB/dotnet/generic_WeakReference.aspx

using System;
using System.Runtime.Serialization;

namespace Bend { 

    /// <summary>
    /// Represents a weak reference, which references an object while still allowing
    /// that object to be reclaimed by garbage collection.
    /// </summary>
    /// <typeparam name="T">The type of the object that is referenced.</typeparam>
    [Serializable]
    public class WeakReference<T>
        : WeakReference where T : class
    {
        /// <summary>
        /// Initializes a new instance of the WeakReference{T} class, referencing
        /// the specified object.
        /// </summary>
        /// <param name="target">The object to reference.</param>
        public WeakReference(T target)
            : base(target) { }
        /// <summary>
        /// Initializes a new instance of the WeakReference{T} class, referencing
        /// the specified object and using the specified resurrection tracking.
        /// </summary>
        /// <param name="target">An object to track.</param>
        /// <param name="trackResurrection">Indicates when to stop tracking the object. 
        /// If true, the object is tracked
        /// after finalization; if false, the object is only tracked 
        /// until finalization.</param>
        public WeakReference(T target, bool trackResurrection)
            : base(target, trackResurrection) { }
        protected WeakReference(SerializationInfo info, StreamingContext context)
            : base(info, context) { }
        /// <summary>
        /// Gets or sets the object (the target) referenced by the 
        /// current WeakReference{T} object.
        /// </summary>
        public new T Target {
            get {
                return (T)base.Target;
            }
            set {
                base.Target = value;
            }
        }
    }

}

namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public partial class T00_UtilTests
    {
        [Test]
        public void T00_TestGenericWeakReference()
        {
            Object o = new Object();
            Object o2 = new Object();
            Bend.WeakReference<Object> wrs = new Bend.WeakReference<Object>(o);

            System.GC.Collect();

            Assert.AreEqual(true, wrs.IsAlive);
            Assert.AreEqual(o, wrs.Target);
            Assert.AreNotEqual(o2, wrs.Target);
            wrs.Target = o2;
            Assert.AreEqual(o2, wrs.Target);
            Assert.AreNotEqual(o, wrs.Target);
            o = null; o2 = null;
            System.GC.Collect();
            System.GC.Collect();
            System.GC.Collect();
            Assert.AreEqual(false, wrs.IsAlive);

            // TODO: test target on dead weakref
        }
    }
}
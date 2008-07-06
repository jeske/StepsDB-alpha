using System;
using System.IO;

// TODO: raw file, raw partition region managers, tests

namespace Bend
{

    // -----------------[ IRegionManager ]---------------------------------------------------
    //
    // This exposes a linear address space called a Region. The LayerManager will divide this
    // Region into segments to hold the root block, log, and segments. We further maintain
    // the invariant that Segments are written in one sweep beginning at their start address. 
    // Once they are closed, they may be read any number of times, but they are disposed of 
    // before being written again. The log does not follow this pattern. 

    public interface IRegionManager
    {
        IRegion readRegionAddr(uint region_addr);
        IRegion readRegionAddrNonExcl(uint region_addr);
        IRegion writeFreshRegionAddr(uint region_addr);
        IRegion writeExistingRegionAddr(uint region_addr);
        void disposeRegionAddr(uint region_addr);
    }

    public interface IRegion : IDisposable
    {
        Stream getStream();
        long getStartAddress();
        long getSize();   // TODO: do something better with this, so we can't break
    }
   

    public interface IRegionWriter : IRegion
    {
        long getMaxSize();
    }
   
    // -----------------[ RegionExposedFiles ]-----------------------------------------------


    // manage a region of exposed files
    class RegionExposedFiles : IRegionManager
    {
        String dir_path;

        
        class EFRegion : IRegion
        {
            FileStream stream;
            long address;
            long length;
            internal EFRegion(long address, long length, FileStream stream) {
                this.address = address;
                this.length = length;
                this.stream = stream;
            }

            public Stream getStream() {
                return stream;
            }

            public long getStartAddress() {
                return address;
            }
            public long getSize() {
                return stream.Length;
            }
            public void Dispose() {
                if (stream != null) {
                    stream.Close();
                    stream = null;
                }
            }
        }
       
        public class RegionMissingException : Exception { 
            public RegionMissingException(String msg) : base(msg) { }
        }
        public RegionExposedFiles(String location) {
            this.dir_path = location;
        }

        // first time init        
        public RegionExposedFiles(InitMode mode, String location) : this(location) {
            if (mode != InitMode.NEW_REGION) {
                throw new Exception("first time init needs NEW_REGION paramater");
            }
            if (!Directory.Exists(dir_path)) {
                Console.WriteLine("LayerManager, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }
        }

        private String makeFilepath(uint region_addr) {
            System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
            String addr = enc.GetString(Lsd.numberToLsd((int)region_addr,13));
            String filepath = dir_path + String.Format("\\addr{0}.reg", addr);
            return filepath;
        }
        // impl ...

        public IRegion readRegionAddr(uint region_addr) {
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {
                FileStream reader = File.Open(filepath, FileMode.Open);
                return new EFRegion(region_addr, reader.Length, reader);
            } else {
                throw new RegionMissingException("no such region address: " + region_addr);
                
            }
        }

        public IRegion readRegionAddrNonExcl(uint region_addr) {
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {
                // open non-exclusive
                FileStream reader = File.Open(filepath, FileMode.Open, FileAccess.Read,FileShare.Read);
                return new EFRegion(region_addr, reader.Length, reader);
            } else {
                throw new RegionMissingException("no such region address: " + region_addr);
            }
        }


        public IRegion writeExistingRegionAddr(uint region_addr) {
            String filepath = makeFilepath(region_addr);
            FileStream writer = File.Open(filepath, FileMode.Open);
            return new EFRegion(region_addr, -1, writer);
        }

        public IRegion writeFreshRegionAddr(uint region_addr) {
            String filepath = makeFilepath(region_addr);
            if (File.Exists(filepath)) {
                this.disposeRegionAddr(region_addr);
            }
            FileStream writer = File.Open(filepath, FileMode.CreateNew);
            return new EFRegion(region_addr,-1,writer);
        }
        public void disposeRegionAddr(uint region_addr) {
            String filepath = this.makeFilepath(region_addr);
            String del_filename = String.Format("\\del{0}addr{1}.region", DateTime.Now.ToBinary(), region_addr);
            File.Move(filepath, dir_path + del_filename);
        }
    }
}


namespace BendTests
{
    using Bend;
    using NUnit.Framework;

    [TestFixture]
    public class A01_RegionExposedFiles
    {
        [Test]
        public void T00_Basic_Region() {


        }


        [Test]
        public void T05_Region_Concurrency() {
        }
    }

}
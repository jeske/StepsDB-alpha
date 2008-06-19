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

    interface IRegionManager
    {
        Stream readRegionAddr(uint region_addr);
        Stream writeRegionAddr(uint region_addr);
        void disposeRegionAddr(uint region_addr);
    }


    // -----------------[ RegionExposedFiles ]-----------------------------------------------


    // manage a region of exposed files
    class RegionExposedFiles : IRegionManager
    {
        String dir_path;
        public RegionExposedFiles(InitMode mode, String location) {
            this.dir_path = location;

            if (!Directory.Exists(dir_path)) {
                Console.WriteLine("LayerManager, creating directory: " + dir_path);
                Directory.CreateDirectory(dir_path);
            }
        }

        public Stream readRegionAddr(uint region_addr) {
            String filename = String.Format("\\addr{0}.reg", region_addr);
            FileStream reader = File.Open(dir_path + filename, FileMode.Open);
            return reader;
        }
        public Stream writeRegionAddr(uint region_addr) {
            String filename = String.Format("\\addr{0}.reg", region_addr);
            FileStream reader = File.Open(dir_path + filename, FileMode.CreateNew);
            return reader;
        }
        public void disposeRegionAddr(uint region_addr) {
            String filename = String.Format("\\addr{0}.reg", DateTime.Now, region_addr);
            String del_filename = String.Format("\\del{0}addr{1}.region", region_addr);
            File.Move(dir_path + filename, dir_path + del_filename);
        }
    }
}
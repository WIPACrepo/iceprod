from icecube import icetray, dataclasses, dataio
import sys
import logging
from I3Tray import I3Units
from icecube.dataclasses import I3Particle


primaries = [
     I3Particle.ParticleType.PPlus      ,
     I3Particle.ParticleType.PMinus     ,
     I3Particle.ParticleType.He4Nucleus ,
     I3Particle.ParticleType.Li7Nucleus ,
     I3Particle.ParticleType.Be9Nucleus ,
     I3Particle.ParticleType.B11Nucleus ,
     I3Particle.ParticleType.C12Nucleus ,
     I3Particle.ParticleType.N14Nucleus ,
     I3Particle.ParticleType.O16Nucleus ,
     I3Particle.ParticleType.F19Nucleus ,
     I3Particle.ParticleType.Ne20Nucleus,
     I3Particle.ParticleType.Na23Nucleus,
     I3Particle.ParticleType.Mg24Nucleus,
     I3Particle.ParticleType.Al27Nucleus,
     I3Particle.ParticleType.Si28Nucleus,
     I3Particle.ParticleType.P31Nucleus ,
     I3Particle.ParticleType.S32Nucleus ,
     I3Particle.ParticleType.Cl35Nucleus,
     I3Particle.ParticleType.Ar40Nucleus,
     I3Particle.ParticleType.K39Nucleus ,
     I3Particle.ParticleType.Ca40Nucleus,
     I3Particle.ParticleType.Sc45Nucleus,
     I3Particle.ParticleType.Ti48Nucleus,
     I3Particle.ParticleType.V51Nucleus ,
     I3Particle.ParticleType.Cr52Nucleus,
     I3Particle.ParticleType.Mn55Nucleus,
     I3Particle.ParticleType.Fe56Nucleus,
     I3Particle.ParticleType.Gamma ]

def IsTrack(particle): 
  track = False
  track = track or particle.GetShape() in [ 
                               I3Particle.ParticleShape.InfiniteTrack, 
                               I3Particle.ParticleShape.StartingTrack, 
                               I3Particle.ParticleShape.StoppingTrack, 
                               I3Particle.ParticleShape.ContainedTrack ]

  track = track or particle.GetType() in [
                               I3Particle.ParticleType.MuPlus,
                               I3Particle.ParticleType.MuMinus,
                               I3Particle.ParticleType.TauPlus,
                               I3Particle.ParticleType.TauMinus,
                               I3Particle.ParticleType.STauPlus,
                               I3Particle.ParticleType.STauMinus,
                               I3Particle.ParticleType.Monopole ]

  track = track or (particle.GetShape() == I3Particle.ParticleShape.Primary and particle.GetType() in primaries)
  return track

def IsCascade(particle): 
  
  csc = False
  csc = csc or particle.GetShape() == I3Particle.ParticleShape.Cascade
  csc = csc or particle.GetType() in [
                               I3Particle.ParticleType.EPlus,
                               I3Particle.ParticleType.EMinus,
                               I3Particle.ParticleType.Brems,
                               I3Particle.ParticleType.DeltaE,
                               I3Particle.ParticleType.PairProd,
                               I3Particle.ParticleType.NuclInt,
                               I3Particle.ParticleType.Hadrons,
                               I3Particle.ParticleType.Pi0,
                               I3Particle.ParticleType.PiPlus,
                               I3Particle.ParticleType.PiMinus]

  csc = csc or (particle.GetShape() == Primary and particle.GetType() in primaries)
  return csc
    

class ZenithWriter(icetray.I3Module):

    def __init__(self, context):
        icetray.I3Module.__init__(self, context)
        self.context       = context
        self.logger        = logging.getLogger("ZenithSorter")

        self.configWritten = False
        self.framecounter  = 0
        self.path          = "physics.%02u.i3"
        self.mcTreeName    = "I3MCTree"
        self.writeconfig   = True
        self.nbin          = 1
        self.zenmin        = 0.0*I3Units.deg
        self.zenmax        = 89.0*I3Units.deg
        self.ofs           = []
        self.bins          = []

        self.AddParameter("Filename","The file we'll write to.", self.path);
        self.AddParameter("MCTreeName","Name of I3MCTree in frame", self.mcTreeName);
        self.AddParameter("Bins","Number of zenith bins",self.nbin);
        self.AddParameter("ZenithMin","Zenith angle lower limit",self.zenmin);
        self.AddParameter("ZenithMax","Zenith angle upper limit",self.zenmax);
        self.AddOutBox("OutBox");


    def Configure(self):
        self.path       = self.GetParameter("Filename")
        self.mcTreeName = self.GetParameter("MCTreeName")
        self.nbin       = self.GetParameter("Bins")
        self.zenmin     = self.GetParameter("ZenithMin")
        self.zenmax     = self.GetParameter("ZenithMax")

        # Compute binning of zenith angular distribution for events
        dzen = (self.zenmax - self.zenmin)/float(self.nbin); 
        self.bins = map(lambda i: self.zenmin+i*dzen, range(self.nbin+1));

        print "Sorting in %u zenith bins with zenmin = %f, zenmax = %f" % (self.nbin, self.zenmin , self.zenmax);
        for  i in range(self.nbin):
           filename = self.path % i;
           self.ofs.append(dataio.I3File(filename, dataio.I3File.Mode.Writing))


    def Physics(self, frame):

        print frame
        isTrack = False;
        zen     = -1;
        zenmin  =  999;
        zenmax  = -999;

        mcList = [];
        particletype = dataclasses.I3Particle.ParticleType.unknown;
        mcTree = frame.Get(self.mcTreeName);

        self.framecounter += 1
        frame.Put("FrameIndex", icetray.I3Int(self.framecounter));

        for cmctrack in mcTree.GetInIce():
		  zen = cmctrack.GetDir().GetZenith();
		  if cmctrack.GetType() > 0: 
		     type = cmctrack.GetType();
		  if zen == zen and ( IsTrack(cmctrack) or IsCascade(cmctrack) ) :
				  zenmin = min(zenmin,zen);
				  zenmax = max(zenmax,zen);
				  isTrack = IsTrack(cmctrack) or IsCascade(cmctrack); 

        if not isTrack and type != I3Particle.ParticleType.unknown : 
           self.logger.error("Unable to find Track or Cascade in particle list! ")
           self.logger.error("particle type is %d" % type )

        for i in range(self.nbin):
		  if  zenmin >= self.bins[i] and zenmin < self.bins[i+1]:
			  self.ofs[i].push(frame);
		  elif  zenmax >= self.bins[i] and zenmax < self.bins[i+1]:
			  self.ofs[i].push(frame);
			  break;

        if  zenmin < self.zenmin or zenmax > self.zenmax : 
		  self.logger.error("zenith %f outside range (%2.1f,%2.1f)" % (zen,self.bins[0],self.bins[self.nbin] ))

        self.PushFrame(frame,"OutBox");
        self.logger.debug("physics... done")

    def Finish(self):
        for file in self.ofs: file.close()
        self.logger.info("FileMerger... done")


class PhysicsFile(dataio.I3File):
    current = None

    def pop_physics(self):
        tmp = self.current
        self.current = dataio.I3File.pop_physics(self)
        return tmp

    def peak(self):
        if not self.current:
           self.current = dataio.I3File.pop_physics(self)
        return self.current

class FileMerger(icetray.I3Module):

    def __init__(self, context):
        icetray.I3Module.__init__(self, context)
        self.logger   = logging.getLogger("FileMerger")
        self.counter  = 0
        self.pattern  = "physics.[0-9]*.i3"

        self.AddParameter("FilePattern","The file we'll write to.", self.pattern);
        self.AddParameter("FileNameList","The file we'll write to.", []);
        self.AddParameter("IndexName","I3Int for merging","FrameIndex");
        self.AddParameter("HitMapName","Name of HitSeriesMap","MCHitSeriesMap");
        self.AddOutBox("OutBox");

    def Configure(self):
        import glob
        self.pattern    = self.GetParameter("FilePattern")
        filelist        = self.GetParameter("FileNameList")
        self.index      = self.GetParameter("IndexName")
        self.hitmap     = self.GetParameter("HitMapName")
        if filelist:
           self.logger.warn("FileNameList was given. FilePattern will be ignored")
        else:
           filelist     = glob.glob(self.pattern)
        print "opening", filelist
        self.ofs = map(lambda x: PhysicsFile(x, dataio.I3File.Mode.Reading), filelist)

    def Process(self):

        indices = [ f.peak().Get(self.index).value for f in self.ofs if f.peak() ]
        if not indices: 
           self.RequestSuspension()
           return

        next = min(indices)
        frames_to_merge = [ f.pop_physics() for f in self.ofs if f.peak() and f.peak().Get(self.index).value == next ]

        # grab the first frame in case they are degenerate
        frame  = frames_to_merge[0]

        # Merge hits from degenerate frames
        if frame.Has(self.hitmap):
           hitmap = reduce(self.MergeHits, map(lambda x: x.Get(self.hitmap), frames_to_merge))
           frame.Delete(self.hitmap)
           frame.Put(self.hitmap,hitmap)
        frame.Delete(self.index)

        if len(frames_to_merge) > 1: print "index",next, ", duplicate frames",len(frames_to_merge)
        self.PushFrame(frame)


    def MergeHits(self,map1, map2):

        if not map2: return map1
        # Add the coinc hits
        for omkey,hits in map2.items():
           if not map1.has_key(omkey):
               map1[omkey] = hits;
           else:
               newSeries = hits;
               for hit in hits:
			      map1[omkey].append(hit);

        for omkey,hits in map1.items():
           hits = list(hits)

           def compare_times(a,b): 
               if a > b:return 1
               elif a == b:return 0
               return -1

           hits.sort(compare_times);
           newhits = dataclasses.vector_I3MCHit()
           for hit in hits: newhits.append(hit)
           map1[omkey] = newhits

        return map1

    def Finish(self):
        for file in self.ofs: file.close()


class HitFilter(icetray.I3Module):

    def __init__(self, context):
        icetray.I3Module.__init__(self, context)
        self.context       = context
        self.logger        = logging.getLogger("sorting.HitFilter")

        self.AddParameter("MCHitSeriesName","Name of HitSeriesMap","MCHitSeriesMap");
        self.AddParameter("Threshold","Minimun number of hits",1);
        self.AddOutBox("OutBox");

    def Configure(self):
        self.hitmap     = self.GetParameter("MCHitSeriesName")
        self.threshold  = self.GetParameter("Threshold")

    def Physics(self, frame):

        if len(frame.Get(self.hitmap).items()) < self.threshold:
           self.logger.debug("Event does not contain enough hits")
           return

        self.PushFrame(frame,"OutBox");
        return



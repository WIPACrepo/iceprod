#include <icetray/I3Tray.h>
#include <icetray/I3TrayInfo.h>
#include <icetray/I3TrayInfoService.h>
#include <icetray/Utility.h>

#include "iceprod-modules/I3MCZenithWriter.h"

#include "icetray/I3Int.h"
#include "dataclasses/I3String.h"
#include "dataclasses/status/I3DetectorStatus.h"
#include "dataclasses/geometry/I3Geometry.h"
#include "dataclasses/physics/I3MCList.h"
#include "dataclasses/physics/I3MCTree.h"
#include "dataclasses/physics/I3MCTreeUtils.h"
#include "dataclasses/physics/I3Particle.h"
#include "dataclasses/calibration/I3Calibration.h"

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <ostream>
#include <set>
#include <cmath>
#include <boost/iostreams/stream.hpp>

#include "iceprod-modules/SimProdUtils.h"

using namespace std;
using boost::archive::portable_binary_oarchive;
namespace io = boost::iostreams;

I3_MODULE(I3MCZenithWriter);

I3MCZenithWriter::I3MCZenithWriter(const I3Context& ctx) : 
  I3Module(ctx),
  configWritten_ (false),
  gzip_compression_level_(-2),
  framecounter_(0),
  path_("physics.%02u.i3"),
  gcspath_(""),
  mcTreeName_("I3MCTree"),
  distribution_("flat"),
  gcsconfigured_(false),
  writeconfig_(true),
  zenithbins_(1),
  zenmin_ ( 0.0*I3Units::deg),
  zenmax_ ( 89.0*I3Units::deg),
  binerr_(1e-6)
{
  binsize_ = zenmax_-zenmin_;

  AddParameter("filename","The file we'll write to.  "
	       "If it ends with .gz and no CompressionLevel is specified, it will be "
	       "gzipped at gzip's default compression level", path_);

  AddParameter("MCTreeName","Name of I3MCTree in frame", mcTreeName_);

  AddParameter("SkipKeys", 
	       "Don't write keys that match any of the regular expressions in this vector", 
	       skip_keys_);
  AddParameter("CompressionLevel", 
	       "0 == no compression, 1 == best speed, 9 == best compression (6 by default)",
	       gzip_compression_level_);
  AddParameter("BinSize","angular width per bin",binsize_);
  AddParameter("ZenithMin","Zenith angle lower limit",zenmin_);
  AddParameter("ZenithMax","Zenith angle upper limit",zenmax_);

  AddParameter("Distribution","String name of angular distribution to apply", 
		  distribution_);
  AddOutBox("OutBox");
}

void I3MCZenithWriter::Configure()
{
  char buffer[100];

  GetParameter("filename", path_);
  GetParameter("MCTreeName", mcTreeName_);
  GetParameter("BinSize",binsize_);
  GetParameter("ZenithMin",zenmin_);
  GetParameter("ZenithMax",zenmax_);
  GetParameter("SkipKeys", skip_keys_);
  GetParameter("CompressionLevel", gzip_compression_level_);
  GetParameter("Distribution",distribution_);


  double dzenithbins_ = (zenmax_ - zenmin_ - binerr_)/(binsize_); 
  zenithbins_ = int(max(ceil(dzenithbins_),1)); 
  bins_ = new double[zenithbins_+1];
  ofs_.resize(zenithbins_);

  // Compute binning of zenith angular distribution for events
  if (distribution_ == "atmo_mu") {
  	AtmoMuDist::compute_bins(
				  zenmin_/I3Units::rad,zenmax_/I3Units::rad, 
				  zenithbins_, bins_);
  }	else if (distribution_ == "flat") {
  	FlatDist::compute_bins(
				  zenmin_/I3Units::rad,zenmax_/I3Units::rad, 
				  zenithbins_, bins_, binsize_ );
  }	else {
	  log_fatal("\"%s\" distribution not implemented.", distribution_.c_str()); 
  }

  log_info("Sorting in %u zenith bins with zenmin = %f, zenmax = %f",zenithbins_, zenmin_ , zenmax_);
  for (int i=0;i<zenithbins_;i++) 
  {
  	sprintf(buffer,path_.c_str(),i); 
	std::string path(buffer);
  	ofs_[i] = EventWriterPtr(new EventWriter()); 
    ofs_[i]->Open(path,skip_keys_);
  }
}

void I3MCZenithWriter::WriteConfig()
{
  if (configWritten_)
    return;

  I3TrayInfoService& srv = 
    context_.Get<I3TrayInfoService>("__tray_info_service");
  const I3TrayInfo& config = srv.GetConfig();
  
  I3TrayInfoPtr trayinfo(new I3TrayInfo(config));

  I3Frame outframe(I3Frame::TrayInfo);
  outframe.Put(trayinfo);

  if (ofs_.begin() != ofs_.end()) {
    ofs_[0]->WriteConfig(outframe);
  }
  configWritten_ = true;
}


void I3MCZenithWriter::Physics(I3FramePtr frame)
{
  WriteConfig();

  I3Frame outframe(frame->GetStop());
  bool isTrack = false;
  double zen = -1;
  double zenmin =  999;
  double zenmax = -999;

  vector<I3Particle> mcList;
  I3Particle::ParticleType type = I3Particle::unknown;
  I3MCTreeConstPtr mcTree = frame->Get<I3MCTreeConstPtr>(mcTreeName_);

  for (I3Frame::const_iterator iter = frame->begin();
       iter != frame->end();
       iter++)
  {
      const std::string &key = iter->first;
      if (key == I3DefaultName<I3Geometry>::value()
          || key == I3DefaultName<I3Calibration>::value()
          || key == I3DefaultName<I3DetectorStatus>::value())
                continue;
      outframe.Put(key, frame->Get<shared_ptr<const I3FrameObject> >(key));
  }
  outframe.Put("FrameIndex", I3IntPtr(new I3Int(++framecounter_)));

  I3MCTree::iterator iter;
  for (iter  = mcTree->begin(); iter!= mcTree->end(); iter++) { 
		  I3Particle cmctrack = *iter; 
		  zen = cmctrack.GetDir().GetZenith();
		  if (cmctrack.GetType() > 0) 
                  type = cmctrack.GetType();
		  if (!isnan(zen) && ( cmctrack.IsTrack() || cmctrack.IsCascade() ) ) {
				  zenmin = min(zenmin,zen);
				  zenmax = max(zenmax,zen);
				  isTrack = cmctrack.IsTrack() || cmctrack.IsCascade(); 
          }
  }
  if (!isTrack && type != I3Particle::unknown ) {
		log_error("Unable to find Track or Cascade in particle list! ");
		log_error("particle type is %d", type); 
  }

  for (int i=0;i<zenithbins_;i++) 
  { 
		  log_trace("? %f > bins[%i]=%f", zen,i,bins_[i]); 
		  if  (zenmin >= bins_[i] && zenmin < bins_[i+1]) {
			  ofs_[i]->WriteFrame(outframe,skip_keys_);
			  break;
		  }
  } 
  if ( zenmin < zenmin_ || zenmax > zenmax_ ) 
		  log_error("zenith %f outside range (%2.1f,%2.1f)",
						zen,bins_[0],bins_[zenithbins_]); 

  PushFrame(frame,"OutBox");
  log_debug("physics... done");  
}


void I3MCZenithWriter::Finish()
{
  I3TrayInfoService& srv = 
    context_.Get<I3TrayInfoService>("__tray_info_service");
  const I3TrayInfo& config = srv.GetConfig();
  I3TrayInfoPtr trayinfo(new I3TrayInfo(config));
  I3Frame outframe(I3Frame::TrayInfo);
  outframe.Put(trayinfo);
  
  for (vector<EventWriterPtr>::iterator iter=ofs_.begin();
       iter != ofs_.end();
       iter++) 
  {
  		EventWriterPtr w = *iter;

  		// Write trayinfo at begining of empty files to keep the reader from 
        // throwing a tantrum
  		if (w->GetCount() == 0){
  		    w->WriteConfig(outframe);
  		    log_debug("writing config for %s", w->GetPath().c_str());  
        }
  		w->Close();
  		log_info("Wrote %i events to '%s'",w->GetCount(),w->GetPath().c_str());  
  }
}


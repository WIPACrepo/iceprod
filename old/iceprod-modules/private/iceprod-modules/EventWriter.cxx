#include "iceprod-modules/EventWriter.h"
#include <icetray/Utility.h>

/*#include "dataio/FrameIO.h"*/
#include <icetray/open.h>

#include "dataclasses/status/I3DetectorStatus.h"
#include "dataclasses/geometry/I3Geometry.h"
#include "dataclasses/calibration/I3Calibration.h"

#include <zlib.h>
#include <ostream>
#include <fstream>
#include <sstream>
#include <set>

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/ref.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>


using namespace std;
using boost::archive::portable_binary_oarchive;
namespace io = boost::iostreams;


EventWriter::EventWriter() : 
  eventCounter_(0),
  path_("Output.i3"),
  gzip_compression_level_(-2) // unset
{
}

void EventWriter::Open(const string path, const std::vector<std::string> skip_keys)
{
  path_ = path;
  skip_keys_ = skip_keys;

  if (path_.rfind(".gz") == (path_.length() - 3)) { // filename ends in .gz
      if (gzip_compression_level_ == -2) // compression level unset 
			  gzip_compression_level_ = 6;    // set to default 
  } else {   // filename doesn't end in .gz 
      if (gzip_compression_level_ == -2) // compression level unset 
			  gzip_compression_level_ = 0; 
  }
  if (gzip_compression_level_ != 0) {
      log_info("Compressing at level %d", gzip_compression_level_);
  } else {
      log_info("Not compressing."); 
  }
  I3::dataio::open(ofs_, path_, gzip_compression_level_);
  
  for (vector<string>::const_iterator iter=skip_keys_.begin();
       iter != skip_keys_.end();
       iter++) 
  {
      log_trace("Will skip entries matching \"%s\"", iter->c_str()); 
  }
}


void EventWriter::WriteFrame(const I3Frame outframe, std::vector<std::string> skip_keys)
{
  //I3::dataio::save<portable_binary_oarchive>(outframe, ofs_,skip_keys);
  outframe.save(ofs_,skip_keys);
  eventCounter_++;
}

void EventWriter::WriteConfig(const I3Frame outframe)
{
  /*I3::dataio::save<portable_binary_oarchive>(outframe, ofs_);*/
  outframe.save(ofs_);
}

void EventWriter::Close()
{
  /*ofs_.flush();*/
  ofs_.reset();
  log_info("Wrote %i events to %s",eventCounter_, path_.c_str());  
}

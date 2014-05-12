#ifndef DATAIO_EVENTREADER_H_INCLUDED
#define DATAIO_EVENTREADER_H_INCLUDED

#include <icetray/IcetrayFwd.h>
#include <boost/iostreams/filtering_stream.hpp>
#include <dataclasses/calibration/I3Calibration.h>
#include <dataclasses/geometry/I3Geometry.h>
#include <dataclasses/status/I3DetectorStatus.h>

#include <fstream>
#include <set>

class EventReader
{
  unsigned nframes_;
  std::string filename_;
  std::vector<std::string> skip_;

  I3FramePtr next_event_;
  std::vector<I3FramePtr> buffer_;

  bool pop_done_;
  bool end_of_file_;

  boost::iostreams::filtering_istream ifs_;
  
  I3DetectorStatusConstPtr status_;
  I3GeometryConstPtr       geometry_;
  I3CalibrationConstPtr    calibration_;
  I3FramePtr metaframe_;

 public:

  EventReader(const std::string& filename, 
		  const std::vector<std::string>& keys_to_skip);

  ~EventReader();

  bool eof();
  I3FramePtr Peek();
  I3FramePtr Pop();

  inline std::string GetName() { return filename_; }

  SET_LOGGER("EventReader");
};

I3_POINTER_TYPEDEFS(EventReader);

#endif

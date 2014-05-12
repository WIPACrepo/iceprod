#ifndef PRODTOOLS_I3MCEVENTREADERSERVICE_H_INCLUDED
#define PRODTOOLS_I3MCEVENTREADERSERVICE_H_INCLUDED

#include <icetray/IcetrayFwd.h>
#include <icetray/I3DefaultName.h>

#include <interfaces/I3EventService.h>
#include <interfaces/I3DetectorStatusService.h>
#include <interfaces/I3CalibrationService.h>
#include <interfaces/I3GeometryService.h>
#include <interfaces/I3MetaService.h>

#include <boost/iostreams/filtering_stream.hpp>
#include <iceprod-modules/EventReader.h>

#include <fstream>
#include <set>


class I3MCEventReaderService : public I3EventService
{
  unsigned nframes_;
  std::vector<std::string> filenames_;
  std::vector<std::string> skip_;

  I3FramePtr next_event_;
  bool pop_done_;
  bool merge_;
  bool skip_unregistered_;
  bool deleteIndex_;

  std::vector<EventReaderPtr> readers_;
  
 public:

  I3MCEventReaderService(
		  const std::vector<std::string>& filenames, 
		  const std::vector<std::string>& keys_to_skip,
		  bool merge_files,
		  bool delIndex);

  ~I3MCEventReaderService();

  // I3EventService
  bool MoreEvents();

  I3Time PopEvent(I3Frame&);

  int compare(I3FramePtr,I3FramePtr);
  I3Time getTime(I3FramePtr& );

};

I3_DEFAULT_NAME(I3MCEventReaderService);
I3_POINTER_TYPEDEFS(I3MCEventReaderService);

#endif

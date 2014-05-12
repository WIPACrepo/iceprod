#include <dataio/FrameIO.h>
#include <iceprod-modules/I3MCEventReaderService.h>
#include <dataclasses/physics/I3EventHeader.h>
#include <icetray/I3Int.h>
#include <icetray/I3TrayInfo.h>
#include <icetray/I3TrayInfoService.h>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <ostream>
#include <boost/iostreams/stream.hpp>

namespace io = boost::iostreams;

I3MCEventReaderService::I3MCEventReaderService(
				 const vector<string>& filenames,
				 const vector<string>& skip,
				 bool merge_files,
				 bool deleteIndex
				 ) 
  : nframes_(0) ,
    pop_done_(true),
    deleteIndex_(deleteIndex)
{
  merge_ = merge_files;
  for (vector<string>::const_iterator iter = filenames.begin();
		  iter != filenames.end(); 
		  iter++)
  { 
  	readers_.push_back(EventReaderPtr(new EventReader((*iter),skip)));
  }
}

I3MCEventReaderService::~I3MCEventReaderService() { }


bool
I3MCEventReaderService::MoreEvents()
{
    if (!pop_done_) 
            return next_event_;

    pop_done_ = false;
	log_debug("iterating through readers");
	int next = -1;
	std::vector<EventReaderPtr>::iterator current_reader = readers_.end(); 
	for (vector<EventReaderPtr>::iterator iter = readers_.begin(); 
                    iter != readers_.end(); iter++) 
    { 
        // flush any non-physics frames at beginning of file
        while ((*iter)->Peek() && (*iter)->Peek()->GetStop() != I3Frame::Physics) 
        {
		    (*iter)->Pop(); 
        }
        if (!(*iter)->Peek()) {
			log_debug("Empty reader."); 
        } else if ((*iter)->Peek()->GetStop() == I3Frame::Physics) {
			const I3Int index = (*iter)->Peek()->Get<I3Int>("FrameIndex"); 
			if (next < 0 || index.value < next) {
			    current_reader = iter;
			    next = index.value;
			} 
        } 
    } 
 
    if (current_reader == readers_.end()) {
		log_trace("no readers left. ");
		return I3FramePtr();
    } else if (!(*current_reader)->Peek()) {
		log_trace("no frames left. ");
		return I3FramePtr(); 
    } else {
		log_debug("reading from %s", (*current_reader)->GetName().c_str());
        if ((*current_reader)->Peek() && (*current_reader)->Peek()->GetStop() == I3Frame::Physics) {
		    next_event_ = (*current_reader)->Pop(); 
        } else {
		    return I3FramePtr(); 
        }
		
    }
    return next_event_;
}

// PopEvent is responsible for keeping the cached other streams 
// current
I3Time 
I3MCEventReaderService::PopEvent(I3Frame& frame)
{
  I3Time thetime(next_event_->Get<I3Time>("DrivingTime"));
  next_event_->Delete("DrivingTime");
  if (deleteIndex_ && next_event_->Has("FrameIndex")) {
          next_event_->Delete("FrameIndex");
  }
  frame.merge(*next_event_);
  pop_done_ = true;
  return thetime;
}


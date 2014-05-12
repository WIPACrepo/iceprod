//#include <dataio/FrameIO.h>
#include <icetray/open.h>
#include <iceprod-modules/EventReader.h>
#include <dataclasses/physics/I3EventHeader.h>
#include <icetray/I3Frame.h>
#include <icetray/I3TrayInfo.h>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <ostream>
#include <boost/iostreams/stream.hpp>

namespace io = boost::iostreams;

EventReader::EventReader(const string& filename, const vector<string>& skip) : 
		nframes_(0), 
		filename_(filename), 
		skip_(skip)
{
  end_of_file_ = false;
  pop_done_ = true;


  log_trace("Constructing with filename %s, %zu regexes", 
	    filename.c_str(), skip_.size());

  I3::dataio::open(ifs_, filename);
  if (!ifs_.good())
    log_fatal("Problem opening file \"%s\" for reading.", filename_.c_str());

}

EventReader::~EventReader() 
{ 
}

I3FramePtr 
EventReader::Pop()
{
	I3FramePtr frame = Peek();
    pop_done_ = true;
	return frame;
}

bool
EventReader::eof()
{ 
	return (ifs_.peek() == EOF);
}

I3FramePtr 
EventReader::Peek()
{
    I3FramePtr frame(new I3Frame());
    if(pop_done_) {
    	pop_done_ = false;
		log_trace("eof?.");
    	if(eof()) {
			log_trace("no more frames.");
			next_event_ = I3FramePtr();
			return next_event_;
        }
		log_trace("eof? no.");
    	try {
			log_trace("load");
			if (frame->load(ifs_, skip_)){
			    next_event_ = frame;
			    nframes_++; 
			    log_trace("load done");
            } else {
			    next_event_ = I3FramePtr();
			    log_trace("no frame found");
            }
		} catch (const boost::archive::archive_exception& e) { 
			// the EOF situation: we return "no more meta" *AND* next_event_ is empty. 
			log_error("caught exception \"%s\" while reading frame %u, no more frames.", 
					e.what(), nframes_); 
			next_event_ = I3FramePtr();
		} 
	}
	return next_event_;
}


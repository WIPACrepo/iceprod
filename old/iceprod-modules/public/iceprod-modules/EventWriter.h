#ifndef I3BOOSTEVENTWRITER_H_INCLUDED
#define I3BOOSTEVENTWRITER_H_INCLUDED

#include <fstream>
#include <string>
#include <icetray/I3TrayHeaders.h>
#include <icetray/I3Logging.h>
#include <boost/iostreams/filtering_stream.hpp>


/**
 *Writes events 
 */
class EventWriter 
{

  /**
   *Assignment operator
   */
  EventWriter& operator=(const EventWriter&);


  /**
   *Number of current event
   */
  int eventCounter_;

  /**
   *Name of current file being written to
   */
  std::string path_;

  std::vector<std::string> skip_keys_;

  /** 
      compression parameters
  */
  int gzip_compression_level_;


  /**
   *Output stream
   */
  boost::iostreams::filtering_ostream ofs_;

  public:
  /**
   *Constructor
   */
  EventWriter(const EventWriter&);

  /**
   *Constructor
   */
  EventWriter();

  /**
   *Destructor
   */
  virtual ~EventWriter() { }

  /**
   *Reads the filename, determines the archive type,
   *and opens the file.  Exits if unsuccessful.
   */
  void Open(const string path, const std::vector<std::string> skip_keys);

  /**
   *Closes the file.
   */
  void Close();

  /**
   */
  void WriteFrame(const I3Frame outframe, const std::vector<std::string> skip_keys);

  void WriteConfig(I3Frame outframe);

  /**
   */
  inline std::string GetPath() { return path_; }

  /**
   */
  inline int GetCount() { return eventCounter_; }


  SET_LOGGER("EventWriter");
};

typedef shared_ptr<EventWriter> EventWriterPtr;

#endif

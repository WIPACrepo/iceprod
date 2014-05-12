#ifndef SIMPROD_I3MCZENITHWRITER_H_INLCUDED
#define SIMPROD_I3MCZENITHWRITER_H_INLCUDED

#include <fstream>
#include <string>
#include <set>
#include <icetray/I3Module.h>
#include <icetray/I3TrayHeaders.h>
#include <icetray/I3Logging.h>
#include "iceprod-modules/EventWriter.h"
#include <dataclasses/I3Units.h>

#include <cmath>
#include <assert.h>


/**
 *Writes physics events to boost files
 */
class I3MCZenithWriter : public I3Module
{
  /**
   *Constructor
   */
  I3MCZenithWriter();
  I3MCZenithWriter(const I3MCZenithWriter&);

  /**
   *Assignment operator
   */
  I3MCZenithWriter& operator=(const I3MCZenithWriter&);

  bool configWritten_;
  int gzip_compression_level_;

  int framecounter_;
  /**
   *Name of current file being written to
   */
  std::string path_;
  std::string gcspath_;
  std::string mcTreeName_;
  std::string distribution_;

  bool gcsconfigured_;
  bool writeconfig_;
  int zenithbins_;
  double zenmin_,zenmax_;
  double binerr_;
  double binsize_;

  /**
   * Array of costheta bins
   */
  double* bins_;

  std::vector<std::string> skip_keys_;

  /**
   *Output stream
   */
  EventWriterPtr gcs_ofs_;
  std::vector<EventWriterPtr> ofs_;

  void WriteConfig();

  public:

  /**
   *Constructor
   */
  I3MCZenithWriter(const I3Context& ctx);

  /**
   *Destructor
   */
  virtual ~I3MCZenithWriter() { }

  /**
   *Reads the filename, determines the archive type,
   *and opens the file.  Exits if unsuccessful.
   */
  void Configure();

  /**
   *Closes the file.
   */
  void Finish();

  /**
   *Gets the Calibration and CalibrationHeader from the 
   *respective streams and writes the pair to a boost file.
   */
  void Physics(I3FramePtr frame);

  SET_LOGGER("I3MCZenithWriter");
};

#endif

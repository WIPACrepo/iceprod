/*
 * class: I3MCEventReaderServiceFactory
 *
 * Version $Id: I3MCEventReaderServiceFactory.cxx 11796 2005-10-24 16:57:39Z juancarlos $
 *
 * Date: 17 Feb 2004
 *
 * (c) IceCube Collaboration
 */

#include "iceprod-modules/I3MCEventReaderServiceFactory.h"


I3_SERVICE_FACTORY(I3MCEventReaderServiceFactory);

#include "iceprod-modules/I3MCEventReaderService.h"
#include <icetray/Utility.h>


I3MCEventReaderServiceFactory::I3MCEventReaderServiceFactory(const I3Context& context) 
  : I3ServiceFactory(context),
    merge_(true),
    deleteIndex_(true)
{
  AddParameter("Filenames", ".i3 file names vector", filenames_);	
  AddParameter("SkipKeys", 
	       "Vector of regexes: if any one matches the key, don't load",
	       skip_keys_);

  AddParameter("MergeFiles",
	       "Merge files instead of reading them in sequential order.",
	       merge_);
  AddParameter("DeleteIndex","Should delete index added by I3MCZenithWriter",deleteIndex_);

}

I3MCEventReaderServiceFactory::~I3MCEventReaderServiceFactory() { }

void 
I3MCEventReaderServiceFactory::Configure()
{

  GetParameter("Filenames", filenames_);
  	log_trace("%d Filenames ", int(filenames_.size()));

  GetParameter("SkipKeys", skip_keys_);

  if(! filenames_.size() )
   	log_fatal("Input file names (FileNames) vector was empty. Needs to be specified");

  GetParameter("MergeFiles",merge_);
  GetParameter("DeleteIndex",deleteIndex_);
}

bool
I3MCEventReaderServiceFactory::InstallService(I3Context& services)
{
  if (!reader_)
    reader_ = I3MCEventReaderServicePtr(
                new I3MCEventReaderService(filenames_,skip_keys_,merge_,deleteIndex_));

  return services.Put<I3EventService>(reader_);

}

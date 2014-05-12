#ifndef PRODTOOLS_I3MCEVENTREADERSERVICEFACTORY_H_INCLUDED
#define PRODTOOLS_I3MCEVENTREADERSERVICEFACTORY_H_INCLUDED

/*
 * class: I3MCEventReaderServiceFactory
 *
 * Version $Id: I3MCEventReaderServiceFactory.h 11148 2005-10-03 21:55:04Z pretz $
 *
 * Date: 17 Feb 2004
 *
 * (c) IceCube Collaboration
 */

class I3Context;

#include "icetray/I3ServiceFactory.h"
#include "iceprod-modules/I3MCEventReaderService.h"

#include <set>

class I3MCEventReaderServiceFactory
: public I3ServiceFactory
{
 public:

  I3MCEventReaderServiceFactory(const I3Context& context);

  virtual ~I3MCEventReaderServiceFactory();

  bool InstallService(I3Context& services);

  void Configure();

 private:

  I3MCEventReaderServiceFactory (const I3MCEventReaderServiceFactory& rhs);
  I3MCEventReaderServiceFactory operator=(const I3MCEventReaderServiceFactory& rhs);

  std::vector<std::string> filenames_;
  std::vector<std::string> skip_keys_;

  I3MCEventReaderServicePtr reader_;

  bool merge_;
  bool deleteIndex_;

  SET_LOGGER("I3MCEventReaderServiceFactory");
};

#endif

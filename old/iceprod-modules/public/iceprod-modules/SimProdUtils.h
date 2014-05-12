#ifndef ANGULARDIST_H_INCLUDED
#define ANGULARDIST_H_INCLUDED

#include <cmath>
#include <gsl/gsl_integration.h>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_math.h>
#include <gsl/gsl_roots.h>
#include <icetray/I3Logging.h>
#include <dataclasses/physics/I3MCHit.h>
#include <assert.h>

#define max(a,b) (a>b)? a : b 
#define min(a,b) (a<b)? a : b 

/**  
  GSL Root Finder
 */
namespace AtmoMuDist{

	typedef struct { 
		double a0;          // angular distribution function params
		double a1; 
		double a2; 
		double lower_limit; // lower_limit used in integration
		double norm;        // normalization constant
		double offset;      // additive constant
		int nbins;
	} dist_params;


	double distribution (double costh, void * p);


	double integrated_distribution (double x, void * p) ;


	void compute_bins(double zenithmin, double zenithmax, 
					int nbin, double *zenbins);

}; // AtmoMuDist namespace 

namespace FlatDist{

	void compute_bins(double zenithmin, double zenithmax, 
					int nbin, double *zenbins, double dzen);
}; // FlatDist



#endif

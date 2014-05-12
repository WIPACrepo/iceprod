#include <cmath>
#include "iceprod-modules/SimProdUtils.h"

using namespace std;


double AtmoMuDist::distribution (double costh, void * p) 
{
   dist_params * params = (dist_params *)p;
   double a0 = (params->a0);
   double a1 = (params->a1);
   double a2 = (params->a2);

   return  (a0 * pow( costh, a1 ) * exp( -a2 /costh ) ) ;

}

double AtmoMuDist::integrated_distribution (double x, void * p) 
{
   double result, error;

   dist_params * params = (dist_params *)p; 
   assert(params->lower_limit >= 0);

   gsl_integration_workspace * w 
		= gsl_integration_workspace_alloc (params->nbins);

   gsl_function gsl_dist; 
   gsl_dist.function = &distribution; 
   gsl_dist.params = params;

   gsl_integration_qags (&gsl_dist, params->lower_limit, x, 0, 1e-7, 
					  params->nbins, w, &result, &error); 

   return (result/params->norm - params->offset);
}


void AtmoMuDist::compute_bins(
			double zenithmin, double zenithmax, 
			int nbin, double* zenbins)
{
  int status, iter, max_iter;
  double min_costh = cos(zenithmax);
  double max_costh = cos(zenithmin);
  double norm;
  double x_lo, x_hi;
  double root = 0;
  const gsl_root_fsolver_type *solver_type;
  gsl_root_fsolver *solver;

  dist_params *dparams = new dist_params;
  solver_type = gsl_root_fsolver_brent;
  solver = gsl_root_fsolver_alloc (solver_type);

  dparams->a0 = 2.49655e-7;
  dparams->a1 = 1.67721; 
  dparams->a2 = 0.778393; 
  dparams->lower_limit = min_costh; 
  dparams->norm = 1.; 
  dparams->offset = 0.; 
  dparams->nbins = 10000;  // number of bins to use for integration

  log_error("min_costh = %f, max_costh = %f", min_costh , max_costh );
  log_error("zenithmin= %f, zenithmax= %f", zenithmin, zenithmax);
  zenbins[0]    = zenithmin;
  zenbins[nbin] = zenithmax;

	  // determine normalization constant
  norm = integrated_distribution(max_costh, dparams);
  dparams->norm = norm;
  dparams->offset = 1./((double) nbin);

  gsl_function gsl_dist_int; 
  gsl_dist_int.function = &integrated_distribution; 
  gsl_dist_int.params = dparams; 

  log_info("using %s method for root solver\n", gsl_root_fsolver_name (solver));
  for (int i=1;i<nbin;i++)
  {
		x_lo = dparams->lower_limit;
		x_hi = 1.0;
		root = 0;
		iter = 0; max_iter = 100;

		gsl_root_fsolver_set (solver, &gsl_dist_int, x_lo, x_hi);

		do 
		{
		  iter++;
		  status = gsl_root_fsolver_iterate (solver);
		  root = gsl_root_fsolver_root (solver);
		  x_lo = gsl_root_fsolver_x_lower (solver);
		  x_hi = gsl_root_fsolver_x_upper (solver);
		  status = gsl_root_test_interval (x_lo, x_hi,0, 0.001);
		} while (status == GSL_CONTINUE && iter < max_iter);

		zenbins[nbin-i]=acos(root);
		dparams->lower_limit = root;
  }
  delete(dparams);

  //return norm;
}

void FlatDist::compute_bins(double zenithmin, double zenithmax, 
					int nbin, double *zenbins, double dzen)
{
	for (int i=0;i<nbin+1;i++)
	{
		zenbins[i]=zenithmin+i*dzen;
	}
}


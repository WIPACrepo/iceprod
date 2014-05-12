#include <I3Test.h>
#include <icetray/I3Tray.h>
#include <icetray/I3Module.h>
#include <icetray/I3TrayHeaders.h>
#include "I3Db/I3OmDb/I3OmDb.h"
#include <icetray/Utility.h>
#include <icetray/modules/TrashCan.h>
#include <boost/program_options.hpp>

namespace po = boost::program_options;
vector<string> libs_to_examine;

void usage(char* cmd, po::options_description options) {
  cerr << "usage: " << cmd << " [options] [outfile] " << "\n";
  cerr << options << "\n";
}


int main(int argc, char** argv)
{
  string outfile;
  vector<string> streams;
  streams.push_back("Geometry");
  streams.push_back("Calibration");
  streams.push_back("DetectorStatus");

  po::options_description generic("Generic options"),
    hidden("Hidden options");

  generic.add_options()
    ("help", "this message")
    ("host,h",po::value<string>(), "hostname of database server")
    ("username,u",po::value<string>(), "username on database server")
    ("database,d",po::value<string>(), "database name")
    ("mjd,m", po::value<int>(),"modified julian date")
    ("password,p",po::value<string>(), "pasword for dbuser")
    ("completegeometry,c",po::value<bool>(), "Complete 80-String Geometry")
    ("amandageometry,a",po::value<bool>(), "Include the Amanda Geometry")
    ("xshift,x",po::value<float>() ,"Shift X coordinate of geometry for efficient simulation")
    ("yshift,y",po::value<float>(), "Shift Y coordinate of geometry for efficient simulation")
    ("runnumber,r",po::value<int>(),
        "Run number to get detector configuration from")
    ("mcsource,m",po::value<bool>(),
        "Run number to get detector configuration from")
    ("outfile,o",po::value<string>(&outfile)->default_value("gcd.i3"), "output file for gcd");


  po::options_description cmdline_opts;
  cmdline_opts.add(generic);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv)
	      .options(cmdline_opts)
		  .run(), vm);

    po::notify(vm);
  } catch (const std::exception& e) {
    cout << argv[0] << ": " << e.what() << "\n";
    usage(argv[0],generic);
    return 1;
  }
  if (argc == 1) {
      usage(argv[0],cmdline_opts);
      return 1;
  }
  if (vm.count("help")) {
    usage(argv[0],cmdline_opts);
    return 1;
  }
  if (!(vm.count("mjd") || vm.count("m"))) {
    usage(argv[0],cmdline_opts);
    return 1;
  } 
  if (!(vm.count("outfile") )) {
    usage(argv[0],cmdline_opts);
    return 1; 
  }
  outfile = vm["outfile"].as<string>();

  // begin I3Tray configuration
  I3Tray tray; 

  // Get OMKey2MBId table
  tray.AddService("I3DbOMKey2MBIDFactory","omkey2mbid");
  if ((vm.count("host") )) {
    tray.SetParameter("omkey2mbid","Host",vm["host"].as<string>());
  }
  if ((vm.count("database") || vm.count("d"))) {
    tray.SetParameter("omkey2mbid","database",vm["database"].as<string>()); 
  }
  if ((vm.count("username") || vm.count("u"))) {
    tray.SetParameter("omkey2mbid","username",vm["username"].as<string>()); 
  }
  if ((vm.count("password") || vm.count("p"))) {
    tray.SetParameter("omkey2mbid","password",vm["password"].as<string>()); 
  }

  // Get OMKey2ChannelId table
  // Only if we are using amanda geometry
  if ((vm.count("amandageometry") )) {
    tray.AddService("I3DbOMKey2ChannelIDFactory","omkey2channelId");
    if ((vm.count("host") )) { 
            tray.SetParameter("omkey2channelId","Host",vm["host"].as<string>()); 
    } 
    if ((vm.count("database") || vm.count("d"))) { 
            tray.SetParameter("omkey2channelId","database",vm["database"].as<string>()); 
    } 
    if ((vm.count("username") || vm.count("u"))) { 
            tray.SetParameter("omkey2channelId","username",vm["username"].as<string>()); 
    } 
    if ((vm.count("password") || vm.count("p"))) { 
            tray.SetParameter("omkey2channelId","password",vm["password"].as<string>()); 
    }

    /*
    // Channel ID
    char* i3build = getenv("I3_BUILD");
    string channel_id_file =  string(i3build) + "/amanda-core/resources/default_geometry.f2k";
    tray.AddService("I3F2kFileChannelID2OMKeyFactory","channelID");
    tray.SetParameter("Infile", channel_id_file);

    string mc_t0_file =  string(i3build) + "/TWRCalibrator/resources/twr_mc_t0_v02_02_01.data"; 
    tray.AddService("I3TWRCalibratorServiceFactory","twrcalib"); 
    tray.SetParameter("UpstreamCalServiceName","I3CalibrationService");
    tray.SetParameter("InstallAs","TWRCal");
    tray.SetParameter("MC",true);
    tray.SetParameter("MCT0CalFile",mc_t0_file);
    */
  }

  // Get Geometry
  tray.AddService("I3DbGeometryServiceFactory","geometry");
  if ((vm.count("database") || vm.count("d"))) {
    tray.SetParameter("geometry","database",vm["database"].as<string>()); 
  }
  if ((vm.count("username") || vm.count("u"))) {
    tray.SetParameter("geometry","username",vm["username"].as<string>()); 
  }
  if ((vm.count("password") || vm.count("p"))) {
    tray.SetParameter("geometry","password",vm["password"].as<string>()); 
  }
  if ((vm.count("completegeometry") )) {
    tray.SetParameter("geometry","completegeometry",vm["completegeometry"].as<bool>()); 
  }
  if ((vm.count("amandageometry") )) {
    tray.SetParameter("geometry","amandageometry",vm["amandageometry"].as<bool>()); 
  }
  if ((vm.count("CustomDate") )) {
    tray.SetParameter("geometry","customdate",vm["customdate"].as<bool>()); 
	tray.SetParameter("geometry","mjd",vm["mjd"].as<int>());
  }
  if ((vm.count("xshift") )) {
	tray.SetParameter("geometry","xshift",vm["xshift"].as<float>()); 
  }
  if ((vm.count("yshift") )) {
	tray.SetParameter("geometry","YShift",vm["yshift"].as<float>()); 
  }
  if ((vm.count("host") )) {
    tray.SetParameter("geometry","Host",vm["host"].as<string>());
  }

  // Get calibration
  tray.AddService("I3DbCalibrationServiceFactory","dbcalibration");
  if ((vm.count("database") || vm.count("d"))) {
    tray.SetParameter("dbcalibration","database",vm["database"].as<string>()); 
  }
  if ((vm.count("username") || vm.count("u"))) {
    tray.SetParameter("dbcalibration","username",vm["username"].as<string>()); 
  }
  if ((vm.count("password") || vm.count("p"))) {
    tray.SetParameter("dbcalibration","password",vm["password"].as<string>()); 
  }
  if ((vm.count("customdate") )) {
    tray.SetParameter("dbcalibration","customdate",vm["customdate"].as<bool>()); 
	tray.SetParameter("dbcalibration","mjd",vm["mjd"].as<int>());
  }
  if ((vm.count("host") )) {
    tray.SetParameter("dbcalibration","host",vm["host"].as<string>());
  }

  // Get detector status
  tray.AddService("I3DbDetectorStatusServiceFactory","dbdetectorstatus"); 
  if ((vm.count("database") || vm.count("d"))) {
    tray.SetParameter("dbdetectorstatus","database",vm["database"].as<string>()); 
  }
  if ((vm.count("username") || vm.count("u"))) {
    tray.SetParameter("dbdetectorstatus","username",vm["username"].as<string>()); 
  }
  if ((vm.count("password") || vm.count("p"))) {
    tray.SetParameter("dbdetectorstatus","password",vm["password"].as<string>()); 
  }
  if ((vm.count("CustomDate") )) {
    tray.SetParameter("dbdetectorstatus","customdate",vm["customdate"].as<bool>()); 
	tray.SetParameter("dbdetectorstatus","mjd",vm["mjd"].as<int>()); 
  }
  if ((vm.count("host") )) {
    tray.SetParameter("dbdetectorstatus","host",vm["host"].as<string>());
  }

  // Setup time generator to keep the muxer happy
  tray.AddService("I3MCTimeGeneratorServiceFactory","time-gen");
  tray.SetParameter("time-gen","mjd",vm["mjd"].as<int>());
  if ((vm.count("runnumber") )) {
    tray.SetParameter("time-gen","runnumber",vm["runnumber"].as<int>());
  }

  
  // Add dummy calib and status values
  if ((vm.count("mcsource") )) {
    tray.AddService("I3MCSourceServiceFactory","mcsource");
    tray.SetParameter("mcsource","GeoServiceName","I3GeometryService");
    tray.SetParameter("mcsource","CalServiceName","DummyCal");
    tray.SetParameter("mcsource","StatusServiceName","DummyStat") ;
  }

  tray.AddModule("I3Muxer","muxer"); 

  // Read dummy calib and status values
  if ((vm.count("mcsource") )) {
    tray.SetParameter("muxer","GeometryService","I3GeometryService");
    tray.SetParameter("muxer","CalibrationService","DummyCal");
    tray.SetParameter("muxer","DetectorStatusService","DummyStat");
  }

  // Write the data
  tray.AddModule("I3Writer","writer");
  tray.SetParameter("writer","FileName",outfile);
  tray.SetParameter("writer","Streams",streams);

  tray.AddModule<TrashCan>("trash");

  tray.Execute(3);
  tray.Finish();
}

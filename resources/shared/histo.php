<html>
<head>
<title>Simulation Production</title>
<meta http-equiv="refresh" content="600" URL="hist.php">
<link href="datahandler.css" rel="stylesheet" type="text/css">
</head>

<body bgcolor=#FFFFFF link=#003366 vlink=#003366>
<div style="font-family:arial,helvetica">
<?php
function showtable($figurelist,$title)
{
  if (sizeof($figurelist) == 0 )  return;
  print "<table width=1000 valign=top>";
  print "<tr>";
  print "<td bgcolor=#003366 colspan=1>";
  print "<font size=5 face=arial color=#FFFFFF>$title</font>";
  print "</td>";
  print "</tr>";
  print "<tr bgcolor=#FFFFFF>";
  print "<td>";
  print "<table width=1000 valign=top>";
  $count = 0;
  $tr_count = 0;
  sort($figurelist);
  foreach ($figurelist as $fname) {

		if ( $count % 4 == 0 ) {
				if ( $count > 0 ) {
					print "</tr>";
					$tr_count--;
				}
				print "<tr>";
				$tr_count++;
		}
		$fullpath = $fname;
		$label = str_replace(".gif","",  $fname);
		$link  = "<img width=240 src='$fullpath'>";
		print "<td><table>";
		print "<tr><td><center><a href='".$_SERVER['PHP_SELF']."?filename=".$fname."'>$link</a></center></td></tr>";
		$label = str_replace(".gif","",  $fname);
		$link  = "<font size=2 color=blue>$label</font>";
		print "<tr><td><center><a href='".$_SERVER['PHP_SELF']."?filename=".$fname."'>$link</a></center></td></tr>";
		print "</table></td>";
		$count++;
  }
  if ($tr_count>0) { print "</tr>";}
  // close the directory
  closedir( $dir );
  print "</table>";
  print "</td>";
  print "</tr>";
  print "</table>";
}

function mktable()
{
  // open the directory
  $dir = opendir(".");
  $count = 0;
  $physicslist = array();
  $detectorlist = array();
  $otherlist = array();

  // loop through it, looking for any/all JPG files:
  while (false !== ($fname = readdir( $dir) )) {
    // parse path for the extension
    $info = pathinfo($fname);
    if ( strtolower($info['extension']) == 'gif' ) {
        if (strpos($fname, 'log') === false) {
            if (strpos($fname, 'phys') !== false) {
		        $physicslist[] = $fname;
             } elseif (strpos($fname, 'det') !== false) {
		        $detectorlist[] = $fname;
             } else {
		        $otherlist[] = $fname;
             }
        }
    }
  }
  showtable($physicslist,"Physics Histograms");
  showtable($detectorlist,"Detector Histograms");
  showtable($otherlist,"Simulation Histograms");
}
?>

<center>

		<?php 
            if ( $_GET["filename"] ) { 
				print '<table width=800 bgcolor=#FFFFFF border=0 style="font-weight=bold;font-size:140%">';
				print '<tr bgcolor=#FFFFFF background=/images/headerbg.jpg>';

               $fname = $_GET['filename'];
               $scale = "linear";
               print "<tr align=center><td><img src=$fname></td></tr>";
               if (strpos($fname, 'log') === false) {
                    $fname = str_replace(".gif", "_log.gif", $_GET['filename']);
                    $scale = "log scale";
               } else {
                    $fname = str_replace("_log.gif", ".gif", $_GET['filename']);
                    $scale = "linear scale";
               }
               print "<tr><td align=center>";
               print "<a href='".$_SERVER['PHP_SELF']."?filename=".$fname."'>";
               print "<font face=arial >switch to $scale</font></a>";
               print "</td></tr>";
            } else {

				print '<table width=800 bgcolor=#003366 border=0 style="font-weight=bold;font-size:140%">';
				print '<tr bgcolor=#FFFFFF background=/images/headerbg.jpg>';
            	print "<td width=790 align=center><font size=7 face=arial color=#FFFFFF>Simulation Production<br> Verification</font></td></tr>";
				print "<tr bgcolor=#003366>";
			  	print "<td align=center colspan=2>";

                mktable(); 

			  	print "</td>";
             	print "</tr></table>";
            }
        ?>
</center>
</body>

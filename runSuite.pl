#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Time::HiRes qw(gettimeofday);

# PROTOTYPES
sub dieWithUsage(;$);

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

# MAIN
dieWithUsage("one or more parameters not defined") unless @ARGV >= 1;
my $suite = shift;
my $scale = shift || 2;
dieWithUsage("suite name required") unless $suite eq "tpcds" or $suite eq "tpch";

chdir $SCRIPT_PATH;
if( $suite eq 'tpcds' ) {
	chdir "sample-queries-tpcds";
} else {
	chdir 'sample-queries-tpch';
} # end if
my @queries = glob '*.sql';

my $db = { 
	'tpcds' => "tpcds_bin_partitioned_orc_$scale",
	'tpch' => "tpch_bin_flat_orc_$scale"
};

print "filename,status,time,rows,jobs\n";
for my $query ( @queries ) {
	my $logname = "$query.log";
	my $cmd="echo 'use $db->{${suite}}; source $query;' | hive -i testbench.settings 2>&1  | tee $query.log";
#	my $cmd="cat $query.log";
	#print $cmd ; exit;
	my ($tsec, $tmsec);
	
	my ($hiveStartSec, $hiveStartUsec) = gettimeofday();

	my @hiveoutput=`$cmd`;
	die "${SCRIPT_NAME}:: ERROR:  hive command unexpectedly exited \$? = '$?', \$! = '$!'" if $?;

	my ($hiveEndSec, $hiveEndUsec) = gettimeofday();
	my $hiveTime = ($hiveEndSec - $hiveStartSec) + ($hiveEndUsec - $hiveStartUsec) / 1000000.0;
	my $fetched = 0;
	my $jobs = 0;
	my $fail = 0;
	foreach my $line ( @hiveoutput ) {
		if( $line =~ /Time taken:\s+([\d\.]+)\s+seconds,\s+Fetched:\s+(\d+)\s+row/ ) {
			$fetched = $fetched + $2;
			$jobs = $jobs + 1;
		} elsif( $line =~ /Time taken:\s+([\d\.]+)\s+seconds/ ) {
			$jobs = $jobs + 1;
		} elsif( 
			$line =~ /^FAILED: /
			# || /Task failed!/ 
			) {
			$fail = 1;
		} # end if
	} # end while
	if ($fail) {
		print "$query,failed,$hiveTime,$fetched,$jobs\n"; 
	} else {
		print "$query,success,$hiveTime,$fetched,$jobs\n"; 
	}
} # end for


sub dieWithUsage(;$) {
	my $err = shift || '';
	if( $err ne '' ) {
		chomp $err;
		$err = "ERROR: $err\n\n";
	} # end if

	print STDERR <<USAGE;
${err}Usage:
	perl ${SCRIPT_NAME} [tpcds|tpch] [scale]

Description:
	This script runs the sample queries and outputs a CSV file of the time it took each query to run.  Also, all hive output is kept as a log file named 'queryXX.sql.log' for each query file of the form 'queryXX.sql'. Defaults to scale of 2.
USAGE
	exit 1;
}


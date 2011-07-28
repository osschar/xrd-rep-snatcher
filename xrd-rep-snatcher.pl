#!/usr/bin/perl -w

use IO::Socket::INET;
use IO::Handle;
use Time::HiRes;

use XML::Simple;

use LWP::Simple  qw();
use Data::Dumper qw();

use ApMon;

# Somewhat short-term solutions
use lib "/etc/xrootd/perllib";
# use lib "perllib";

use Getopt::FileConfig;

our ($HELP, $PORT, $APMON_HOST, $APMON_PORT, $CONFIG_URL,
     $CLUSTER_PREFIX,
     $LOG_FILE, $LOG_LEVEL);

my $cfg_parser = new Getopt::FileConfig(-defcfg=>"/etc/xrootd/xrd-rep-snatcher.rc");
$cfg_parser->parse();

if ($HELP)
{
  print "usage: $0 <options>\n";
  print "  Options (from $cfg_parser->{'Config'}):\n";
  open CFG, $cfg_parser->{'Config'};
  my @ls = <CFG>; print @ls;
  close CFG;
  exit 0;
}

# flush after every write
# $| = 1;


################################################################################
# globals set for each record
################################################################################

$G_Pgm     = "none";
$G_Host    = "none";
$G_Site    = "none";

$G_Cluster = "none";

# Only set when old values exist, too
$G_Delta_T = 0;

# Accumulated over whole record
$G_Result  = ();


################################################################################
# config
################################################################################

$G_Host2Site_Default =
{
  'ucsd.edu'       => 'UCSD',
  'unl.edu'        => 'UNL',
  'ultralight.org' => 'CalTech',
  'fnal.gov'       => 'FNAL',
  'purdue.edu'     => 'Purdue',
  'wisc.edu'       => 'Wisconsin',
};

sub load_remote_config
{
  undef $G_Host2Site;

  eval LWP::Simple::get($CONFIG_URL);

  print LOG "Loading of remote config from $CONFIG_URL ";
  if (defined $G_Host2Site)
  {
    $G_Host2Site_Default = $G_Host2Site;
    print LOG "successful:\n";
  }
  else
  {
    $G_Host2Site = $G_Host2Site_Default;
    print LOG "failed, kept old or default values:\n";
  }
  print LOG "  ", Data::Dumper::Dumper($G_Host2Site);

  LOG->flush();

  $reload_remote_config = 0;
}

sub open_log_file
{
  close LOG if defined(LOG);
  open  LOG, ">> $LOG_FILE" or die "Can not open logfile '$LOG_FILE'.";
}


################################################################################
# xrootd / cmsd config of stuff to send to ML
################################################################################

$Pgm2ClusterPostfix =
{
  'xrootd' => '::XrdReport',
  'cmsd'   => '::CmsdReport',
};

$Pgm2Values =
{
  'xrootd' =>
   [
    [ ['buff'],  ['reqs', 'buffs', 'mem'] ],
    [ ['link'],  ['ctime', 'maxn', 'in', 'num', "out", "tmo", "tot"] ],
    # ofs - all zeroes?
    # oss - not used!
    # ['poll'], ['att', 'en', 'ev', 'int']
    # proc - only as rates
    [ ['sched'], ['idle', 'inq', 'maxinq', 'tcr', 'tde', 'threads', 'tlimr'] ],
   ],
  'cmsd' =>
   [
    # [  ],
   ],
};

$Pgm2Rates =
{
  'xrootd' => [
    [ ['buff'], ['reqs', 'buffs', 'mem'] ],
    [ ['link'], ['in', 'num', "out", "tmo", "tot"] ],
    # ['poll'], ['att', 'en', 'ev', 'int']
    [ ['proc'], ['sys', 'usr'] ],
    [ ['sched'], ['jobs'] ],
    [ ['xrootd', 'ops'], ['getf', 'misc', 'open', 'pr', 'putf', 'rd', 'rf', 'sync', 'wr'] ],
  ],
  'cmsd'   => [
    [ ['proc'], ['sys', 'usr']  ],
    # [  ],
  ],
};


################################################################################
# print, compare sub-trees
################################################################################

sub print_log($@)
{
  my ($l, @a) = @_;

  if ($LOG_LEVEL >= $l)
  {
    print LOG @a;
  }
}

sub print_compare_entries
{
  my ($d, $o, $path) = @_;

  $d = $d->{stats};
  $o = $o->{stats};

  for $k (@$path)
  {
    $d = $d->{$k};
    $o = $o->{$k};
  }

  print LOG "  ", join('.', @$path), "\n";
  for $k (sort keys(%$d))
  {
    printf LOG "    %-10s  %20f  %20f  %20f %20f\n", $k, $d->{$k}, $o->{$k},
      $d->{$k} - $o->{$k}, ($d->{$k} - $o->{$k}) / $G_Delta_T;
  }
}


################################################################################
# ApMon senders
################################################################################

sub send_values
{
  my ($d, $path, $params) = @_;

  $d = $d->{stats};

  for $k (@$path)
  {
    $d = $d->{$k};
  }

  my $prefix = join('_', @$path);
  my @result = ();

  for $p (@$params)
  {
    push @result, $prefix . "_" . $p, $d->{$p};
  }

  print_log 2, "Sending values:", join(', ', @result), "\n";

  push @G_Result, @result;
}

sub send_rates
{
  my ($d, $o, $path, $params) = @_;

  $d = $d->{stats};
  $o = $o->{stats};

  for $k (@$path)
  {
    $d = $d->{$k};
    $o = $o->{$k};
  }

  my $prefix = join('_', @$path);
  my @result = ();

  # print "  $prefix\n";
  for $p (@$params)
  {
    # printf LOG "    %-8s  %20f  %20f  %20f\n", $p, $d->{$p}, $o->{$p}, $d->{$p} - $o->{$p};
    push @result, $prefix . "_" . $p . "_R", ($d->{$p} - $o->{$p}) / $G_Delta_T;
  }

  print_log 2, "Sending rates:", join(', ', @result), "\n";

  push @G_Result, @result;
}


################################################################################
# main()
################################################################################

open_log_file();
load_remote_config();

$SIG{HUP} = sub { $reload_remote_config = 1; };

my $prev_vals = {};

my $xml = new XML::Simple;

my $socket = new IO::Socket::INET(LocalPort => $PORT, Proto => 'udp')
    or die "ERROR in Socket Creation: $!\n";

my $apmon = 0;
if ($APMON_PORT != 0)
{
  $apmon = new ApMon({ "${APMON_HOST}:${APMON_PORT}" =>
		       { "sys_monitoring" => 0,
			 "general_info" => 0
		       }
		     });
}
else
{
  print LOG "APMON_PORT = 0 -- will not publish any data!\n";
}

while (1)
{
  load_remote_config() if $reload_remote_config;

  # read operation on the socket
  my $raw_data;
  next unless defined $socket->recv($raw_data, 8192);

  my $recv_time = Time::HiRes::time();

  # get the peerhost and peerport at which the recent data received.
  my $address = $socket->peerhost();
  my $port    = $socket->peerport();

  # print LOG "\n($address , $port) said : $raw_data\n\n";

  my $d = $xml->XMLin($raw_data, keyattr => ["id"]);

  ### next unless $d->{src} eq 'uaf-3.t2.ucsd.edu:1094';

  next unless exists $d->{stats};

  # Set receive time
  $d->{tor} = $recv_time;
  # Fix bug in sgen/toe storage:
  if (exists $d->{stats}{sgen}{toe})
  {
    $d->{toe} = $d->{stats}{sgen}{toe};
    delete $d->{stats}{sgen}{toe};
  }
  # Gather up sec/musec reports
  if (exists $d->{stats}{proc})
  {
    $d->{stats}{proc}{sys} = $d->{stats}{proc}{sys}{s} + 0.000001 * $d->{stats}{proc}{sys}{u};
    $d->{stats}{proc}{usr} = $d->{stats}{proc}{usr}{s} + 0.000001 * $d->{stats}{proc}{usr}{u};
  }

  $G_Pgm  = $d->{pgm};
  $G_Host = $d->{stats}{info}{host};

  $G_Host =~ m/(\w+\.\w+)$/;
  $G_Site = exists $G_Host2Site->{$1} ? $G_Host2Site->{$1} : 'unknown';

  print_log 0,
  "Message from $d->{src}, len=", length $raw_data, ", Site=$G_Site, Pgm=$G_Pgm\n",
  "  Local time:    ", scalar localtime $d->{tor}, "\n",
  "  Recv time:     ", $d->{tor}, "\n",
  "  Service start: ", $d->{tos}, "\n",
  "  Collect start: ", $d->{tod}, ", end:", $d->{toe}, ", delta=", $d->{toe} - $d->{tod}, "\n";

  if (not exists $Pgm2ClusterPostfix->{$G_Pgm})
  {
    print_log 0, "  Dropping -- unknown program name.";
    next;
  }

  if ($G_Site eq 'none')
  {
    print_log 0, "  Dropping -- unknown site.";
    next;
  }

  $G_Cluster = ${CLUSTER_PREFIX} . ${G_Site} . $Pgm2ClusterPostfix->{$d->{pgm}};

  # print LOG $raw_data, "\n";
  print_log 3, Data::Dumper::Dumper($d);

  ### Process variables

  for $value_pair (@{$Pgm2Values->{$G_Pgm}})
  {
    send_values($d, $value_pair->[0], $value_pair->[1]);
  }

  ### Process rates (requires previous record and the same service start time)

  if (exists $prev_vals->{$d->{src}} and $d->{tos} == $prev_vals->{$d->{src}}{tos})
  {
    my $o = $prev_vals->{$d->{src}};

    $G_Delta_T = $d->{tor} - $o->{tor};

    print_log 1, "  Has prev val, time was $o->{tor}, delta=$G_Delta_T\n";

    #print_compare_entries($d, $o, ['buff']);
    #print_compare_entries($d, $o, ['link']);
    #print_compare_entries($d, $o, ['poll']);
    #print_compare_entries($d, $o, ['proc']);
    #print_compare_entries($d, $o, ['sched']);
    #print_compare_entries($d, $o, ['xrootd', 'aio']);
    #print_compare_entries($d, $o, ['xrootd', 'ops']);

    for $value_pair (@{$Pgm2Rates->{$G_Pgm}})
    {
      send_rates($d, $o, $value_pair->[0], $value_pair->[1]);
    }

    $G_Delta_T = 0;
  }

  if ($apmon)
  {
    $apmon->sendParameters($G_Cluster, $G_Host, @G_Result);
  }

  @G_Result = ();

  $prev_vals->{$d->{src}} = $d;

  LOG->flush();
}

$socket->close();

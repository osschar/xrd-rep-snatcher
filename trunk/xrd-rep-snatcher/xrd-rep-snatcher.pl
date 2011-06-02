#!/usr/bin/perl -w

use IO::Socket::INET;
use IO::Handle;
use Time::HiRes;

use XML::Simple;

use LWP::Simple  qw();
use Data::Dumper qw();

use ApMon;

# Somewhat short-term solution
use lib "/etc/xrootd/perllib";
# use lib "perllib";

use Getopt::FileConfig;

our ($HELP, $PORT, $APMON_HOST, $APMON_PORT, $CONFIG_URL,
     $CLUSTER_PREFIX, $CLUSTER_POSTFIX,
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
# print, compare sub-trees
################################################################################

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

  # print LOG "Sending values:", join(', ', @result), "\n";

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

  # print LOG "Sending rates:", join(', ', @result), "\n";

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

  next unless $d->{pgm} eq 'xrootd' and exists $d->{stats};

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

  $G_Host = $d->{stats}{info}{host};

  $G_Host =~ m/(\w+\.\w+)$/;
  $G_Site = exists $G_Host2Site->{$1} ? $G_Host2Site->{$1} : 'unknown';

  print LOG "Message from $d->{src}, len=", length $raw_data, ", Site=$G_Site\n";
  print LOG "  Local time:    ", scalar localtime $d->{tor}, "\n";
  print LOG "  Recv time:     ", $d->{tor}, "\n";
  print LOG "  Service start: ", $d->{tos}, "\n";
  print LOG "  Collect start: ", $d->{tod}, ", end:", $d->{toe}, ", delta=", $d->{toe} - $d->{tod}, "\n";

  if ($G_Site eq 'none')
  {
    print LOG "  Dropping -- unknown site.";
    next;
  }

  $G_Cluster = ${CLUSTER_PREFIX} . ${G_Site} . ${CLUSTER_POSTFIX};

  # print LOG $raw_data, "\n";
  print LOG Data::Dumper::Dumper($d);

  ### Process variables

  send_values($d, ['buff'], ['reqs', 'buffs', 'mem']);
  send_values($d, ['link'], ['ctime', 'maxn', 'in', 'num', "out", "tmo", "tot"]);
  # ofs - all zeroes?
  # oss - not used!
  # send_values($d, ['poll'], ['att', 'en', 'ev', 'int']);
  # proc - only as rates
  send_values($d, ['sched'], ['idle', 'inq', 'maxinq', 'tcr', 'tde', 'threads', 'tlimr']);
  # send_values($d, ['xrootd', 'aio'], []);
  # send_values($d, ['xrootd', 'ops'], []);

  ### Process rates (requires previous record and the same service start time)

  if (exists $prev_vals->{$d->{src}} and $d->{tos} == $prev_vals->{$d->{src}}{tos})
  {
    my $o = $prev_vals->{$d->{src}};

    $G_Delta_T = $d->{tor} - $o->{tor};

    print LOG "  Has prev val, time was $o->{tor}, delta=$G_Delta_T\n";

    #print_compare_entries($d, $o, ['buff']);
    #print_compare_entries($d, $o, ['link']);
    #print_compare_entries($d, $o, ['poll']);
    #print_compare_entries($d, $o, ['proc']);
    #print_compare_entries($d, $o, ['sched']);
    #print_compare_entries($d, $o, ['xrootd', 'aio']);
    #print_compare_entries($d, $o, ['xrootd', 'ops']);

    send_rates($d, $o, ['buff'], ['reqs', 'buffs', 'mem']);
    send_rates($d, $o, ['link'], ['in', 'num', "out", "tmo", "tot"]);
    # send_rates($d, $o, ['poll'], ['att', 'en', 'ev', 'int']);
    send_rates($d, $o, ['proc'], ['sys', 'usr']);
    send_rates($d, $o, ['sched'], ['jobs']);
    send_rates($d, $o, ['xrootd', 'ops'], ['getf', 'misc', 'open', 'pr', 'putf', 'rd', 'rf', 'sync', 'wr']);

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

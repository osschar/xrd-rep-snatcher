#!/usr/bin/perl -w

################################################################################
# xrd-rep-snatcher -- reads xrootd/cmsd report-style monitoring and passes it
#                     on to MonALISA
# Author: Matevz Tadel, 2011; mtadel@ucsd.edu
################################################################################

use POSIX ();
use Proc::Daemon;

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
     $CLUSTER_PREFIX, $DAEMON, $LOG_FILE, $LOG_LEVEL);

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
$G_ProcId  = "none";

$G_Cluster = "none";

# Only set when old values exist, too
$G_Delta_T = 0;

# Accumulated over whole record
$G_Result  = ();


################################################################################
# Logging
################################################################################

sub open_log_file
{
  close LOG if defined(LOG) and *LOG ne *STDOUT;
  if ($LOG_FILE eq '-')
  {
    *LOG = *STDOUT;
  }
  else
  {
    open  LOG, ">> $LOG_FILE" or die "Can not open logfile '$LOG_FILE'.";
  }
}

sub print_log($@)
{
  my ($l, @a) = @_;

  if ($LOG_LEVEL >= $l)
  {
    my $now = localtime();
    $now =~ s/^\S+\s((\S+\s+){3}).*$/$1/o;
    print LOG $now, @a;
  }
}

sub put_log($@)
{
  my ($l, @a) = @_;

  if ($LOG_LEVEL >= $l)
  {
    print LOG ' ' x 16, @a;
  }
}

################################################################################
# config
################################################################################

$G_Host2Site_Default =
{
  # Common sites
  'cern.ch'        => 'CERN',

  # CMS sites
  'ucsd.edu'       => 'UCSD',
  'unl.edu'        => 'UNL',
  'ultralight.org' => 'CalTech',
  'fnal.gov'       => 'FNAL',
  'purdue.edu'     => 'Purdue',
  'wisc.edu'       =>  sub {
    my $h = shift;
    return 'Wisconsin' if $h =~ m/^s17n\d+\.hep\./;
    return 'Wisconsin Internal';
  },
  'ufl.edu'        => 'UFL',
  'mit.edu'        => 'MIT',
  'ac.uk'          =>  sub {
    my $h = shift;
    return 'UK IC London' if $h =~ m/\.ic\.ac\.uk/;
    return 'UK RAL'       if $h =~ m/\.rl\.ac\.uk/;
    return 'UK';
  },
  'infn.it'        =>  sub {
    my $h = shift;
    return 'INFN Bari'    if $h =~ m/\.ba\.infn\.it/;
    return 'INFN Pisa'    if $h =~ m/\.pi\.infn\.it/;
    return 'INFN Legnaro' if $h =~ m/\.lnl\.infn\.it/;
    return 'INFN';
  },

  # ATLAS sites
  'stanford.edu'   => 'SLAC',
  'aglt2.org'      => 'AGLT2',
  'atlas-swt2.org' => 'SWT2_CPB',
  'ou.edu'         =>  sub {
    my $h = shift;
    return 'SWT2_OU_T3' if $h =~ m/\.nhn\.ou\.edu/;
    return 'SWT2_OU';
  },
  'iu.edu'         => 'MWT2_IU',
  'uiuc.edu'       => 'MWT2_UIUC',
  'uchicago.edu'   => 'MWT2_UC',
};

sub load_config_file
{
  undef $G_Host2Site;

  if ($CONFIG_URL =~ m!^https?:!)
  {
    eval LWP::Simple::get($CONFIG_URL);
  }
  else
  {
    do $CONFIG_URL;
  }

  my $log = "Loading config from $CONFIG_URL ";
  if (defined $G_Host2Site)
  {
    $G_Host2Site_Default = $G_Host2Site;
    $log .= "successful:\n";
  }
  else
  {
    $G_Host2Site = $G_Host2Site_Default;
    $log .= "failed, kept old or default values:\n";
  }
  print_log 0, $log, "  ", Data::Dumper::Dumper($G_Host2Site);
  LOG->flush();
}

sub determine_site_name
{
  my $host = shift;

  $host =~ m/([\w-]+\.[\w-]+)$/;
  my $domain = $1;

  if (exists $G_Host2Site->{$domain})
  {
    my $e = $G_Host2Site->{$domain};
    my $t = ref($e);

    if (not $t)
    {
      return $e;
    }
    elsif ($t eq 'CODE')
    {
      return &$e($host);
    }
    else
    {
      print_log 0, "Unknown reference type '$t' for host $host.";
      LOG->flush();
    }
  }

  if ($UNKNOWN_SITE eq '<domain>')
  {
     $host =~ m/^[^\.]\.(.*)$/;
     return $1;
  }
  else
  {
    return $UNKNOWN_SITE;
  }
}


################################################################################
# xrootd / cmsd config of stuff to send to ML
################################################################################

$Pgm2ClusterPostfix =
{
  'xrootd' => '::XrdReport',
  'cmsd'   => '::CmsdReport',
};

### NOTE THIS: 'stats' is implied unless first array-ref is empty!
### This is to allow extraction of top-levels, like 'ver' in this case.
$Pgm2Values =
{
  'xrootd' => [
    [ [],        ['ver', 'site', 'pid'] ],
    [ ['buff'],  ['reqs', 'buffs', 'mem'] ],
    [ ['link'],  ['ctime', 'maxn', 'in', 'num', 'out', 'tmo', 'tot'] ],
    [ ['sched'], ['idle', 'inq', 'maxinq', 'tcr', 'tde', 'threads', 'tlimr'] ],
   ],
  'cmsd' => [
    [ [],        ['ver', 'site', 'pid'] ],
   ],
};

$Pgm2Rates =
{
  'xrootd' => [
    [ ['buff'],          ['reqs', 'buffs', 'mem'] ],
    [ ['link'],          ['in', 'num', 'out', 'tmo', 'tot'] ],
    [ ['proc'],          ['sys', 'usr'] ],
    [ ['sched'],         ['jobs'] ],
    [ ['xrootd'],        ['num', 'dly', 'err', 'rdr'] ],
    [ ['xrootd', 'ops'], ['getf', 'misc', 'open', 'pr', 'putf', 'rd', 'rf', 'sync', 'wr'] ],
    [ ['xrootd', 'lgn'], ['num', 'af', 'au', 'ua'] ],
  ],
  'cmsd' => [
    [ ['proc'],          ['sys', 'usr']  ],
    [ ['cmsm','frq'],    ['rs', 'rsp_m', 'add', 'ls', 'rf', 'add_d', 'rsp', 'lf'] ],
  ],
};


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
    last unless exists $d->{$k};
    $d = $d->{$k};
    last unless exists $o->{$k};
    $o = $o->{$k};
  }

  print LOG "  ", join('.', @$path), "\n";
  for $k (sort keys(%$d))
  {
    next unless exists $d->{$k} and exists $o->{$k};
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

  my $prefix = "";

  if (scalar @$path > 0)
  {
    ### NOTE THIS, implied cd into 'stats'!
    $d = $d->{stats};

    for $k (@$path)
    {
      last unless exists $d->{$k};
      $d = $d->{$k};
    }

    $prefix = join('_', @$path) . '_';
  }

  my @result = ();

  for $p (@$params)
  {
    next unless exists $d->{$p};
    push @result, $prefix . $p, $d->{$p};
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
    next unless exists $d->{$p} and exists $o->{$p};
    # printf LOG "    %-8s  %20f  %20f  %20f\n", $p, $d->{$p}, $o->{$p}, $d->{$p} - $o->{$p};
    push @result, $prefix . "_" . $p . "_R", ($d->{$p} - $o->{$p}) / $G_Delta_T;
  }

  print_log 2, "Sending rates:", join(', ', @result), "\n";

  push @G_Result, @result;
}


################################################################################
# Signal handlers
################################################################################

my $sig_hup_received  = 0;
my $sig_term_received = 0;

sub sig_moo_handler
{
  my $sig = shift;

  if ($sig eq 'HUP')
  {
    print_log 0, "SigHUP received ...\n"; LOG->flush();
    $sig_hup_received = 1;
  }
  elsif ($sig eq 'CHLD')
  {
    print_log 0, "SigCHLD received.\n"; LOG->flush();
  }
}

sub sig_term_handler
{
  print_log 0, "SigTERM received ... presumably exiting.\n"; LOG->flush();
  $sig_term_received = 1;
}


################################################################################
# main()
################################################################################

# Try to open the log file.
open_log_file();

if ($DAEMON)
{
  my $pid = Proc::Daemon::Init({
    work_dir      => "/",                      # This is default, too.
    dont_close_fh => ['ApMon::Common::SOCKET'] # Bummer, ApMon has static socket init!
  });

  if ($pid)
  {
    open  PF, ">$PID_FILE" or die "Can not open pid file, dying";
    print PF $pid, "\n";
    close PF;
    exit 0;
  }
  else
  {
    print "Yow, i R the daemon, we guess\n";
    # Reopen log for the new process.
    open_log_file();
    print_log 0, "$0 starting.\n";
    print_log 0, "Redirecting stdout and stderr into this file.\n";
    *STDOUT = *LOG;
    *STDERR = *LOG;
  }
}


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
  print_log 0, "APMON_PORT = 0 -- will not publish any data!\n";
}


# Install sig handlers now ... ApMon messes this up.

my $sigact1 = POSIX::SigAction->new(\&sig_moo_handler, POSIX::SigSet->new());
POSIX::sigaction(&POSIX::SIGHUP,  $sigact1);
POSIX::sigaction(&POSIX::SIGCHLD, $sigact1);

my $sigact2 = POSIX::SigAction->new(\&sig_term_handler, POSIX::SigSet->new());
POSIX::sigaction(&POSIX::SIGTERM, $sigact2);

print_log 0, "Installed signal handlers.\n";


# Get ready for work ...

load_config_file();
print_log 0, "Loaded configuration from file.\n";


my $prev_vals = {};

my $xml = new XML::Simple;

my $socket = new IO::Socket::INET(LocalPort => $PORT, Proto => 'udp')
    or print_log 0, "ERROR in Socket Creation: $!\n", exit 1;


# The main loop

while (not $sig_term_received)
{
  if ($sig_hup_received)
  {
    print_log 0, "Processing SigHUP: reopening log file.\n";
    open_log_file();
    print_log 0, "Processing SigHUP: log file reopened.\n";
    print_log 0, "Processing SigHUP: reloading configuration from file.\n";
    load_config_file();
    $sig_hup_received = 0;
  }

  # read operation on the socket
  my $raw_data;
  next unless defined $socket->recv($raw_data, 8192);

  my $recv_time = Time::HiRes::time();

  # get the peerhost and peerport at which the recent data received.
  my $address = $socket->peerhost();
  my $port    = $socket->peerport();

  # print LOG "\n($address , $port) said : $raw_data\n\n";

  my $d = eval { $xml->XMLin($raw_data, keyattr => ["id"]); };

  if ($@)
  {
    print_log 1, "Parse error on msg from $address:$port:", $@, $raw_data, "\n";
    next;
  }

  # next unless $d->{src} =~ m/^xrootd.t2.ucsd.edu/ and $d->{pgm} eq 'cmsd';

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
  # Fix-up cmsm_frq (maybe could do it with XML::Simple options?)
  if ($d->{pgm} eq 'cmsd' and exists $d->{stats}{cmsm}{frq})
  {
    $d->{stats}{cmsm}{frq}{add_d} = $d->{stats}{cmsm}{frq}{add}{d};
    $d->{stats}{cmsm}{frq}{add}   = $d->{stats}{cmsm}{frq}{add}{content};
    $d->{stats}{cmsm}{frq}{rsp_m} = $d->{stats}{cmsm}{frq}{rsp}{m};
    $d->{stats}{cmsm}{frq}{rsp}   = $d->{stats}{cmsm}{frq}{rsp}{content};
  }

  $G_Pgm  = $d->{pgm};
  $G_Host = $d->{stats}{info}{host};
  $G_Site = determine_site_name($G_Host);
  $G_ProcId = "$d->{src}_$d->{pgm}_$d->{tos}";

  my $cluster_pfx = exists $Pgm2ClusterPostfix->{$G_Pgm} ? $Pgm2ClusterPostfix->{$G_Pgm} : 'unknown';
  $G_Cluster = ${CLUSTER_PREFIX} . ${G_Site} . $cluster_pfx;

  print_log 0, "Message from $d->{src}, len=", length $raw_data, ", Site=$G_Site, Pgm=$G_Pgm, Cluster=$G_Cluster\n";
  put_log   2, "Service start: ", $d->{tos}, "\n";
  put_log   2, "Collect start: ", $d->{tod}, ", end:", $d->{toe}, ", delta=", $d->{toe} - $d->{tod}, "\n";

  if ($cluster_pfx eq 'unknown')
  {
    put_log 0, "  Dropping -- unknown program name.\n";
    next;
  }

  # print LOG $raw_data, "\n";
  # print LOG Data::Dumper::Dumper($d);

  ### Process variables

  for $value_pair (@{$Pgm2Values->{$G_Pgm}})
  {
    send_values($d, $value_pair->[0], $value_pair->[1]);
  }

  ### Process rates (requires previous record and the same service start time)

  if (exists $prev_vals->{$G_ProcId})
  {
    my $o = $prev_vals->{$G_ProcId};

    $G_Delta_T = $d->{tor} - $o->{tor};

    put_log 1, "Has prev val, time was $o->{tor}, delta=$G_Delta_T\n";

    #print_compare_entries($d, $o, ['buff']);
    #print_compare_entries($d, $o, ['link']);
    #print_compare_entries($d, $o, ['poll']);
    #print_compare_entries($d, $o, ['proc']);
    #print_compare_entries($d, $o, ['sched']);
    #print_compare_entries($d, $o, ['xrootd', 'aio']);
    #print_compare_entries($d, $o, ['xrootd', 'ops']);

    push @G_Result, "delta_t", $G_Delta_T;

    for $value_pair (@{$Pgm2Rates->{$G_Pgm}})
    {
      send_rates($d, $o, $value_pair->[0], $value_pair->[1]);
    }

    $G_Delta_T = 0;
  }
  else
  {
    push @G_Result, "delta_t", -1;
  }

  if ($apmon)
  {
    $apmon->sendParameters($G_Cluster, $G_Host, @G_Result);
  }

  @G_Result = ();

  $prev_vals->{$G_ProcId} = $d;

  LOG->flush();
}


print_log 0, "Out of main loop ... shutting down.\n";

$socket->close();

if ($DAEMON)
{
  print_log 0, "Removing pid-file $PID_FILE.\n";

  unlink $PID_FILE;
}

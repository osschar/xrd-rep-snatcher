#!/usr/bin/perl

# SELECT relname, relpages FROM pg_class WHERE relpages > 1000000 ORDER BY relpages DESC;

# mon_data=> SELECT relname, relpages FROM pg_class WHERE relname LIKE '%_delta_t' OR relname LIKE '%_pid' ORDER BY relpages DESC;
#      relname     | relpages
# -----------------+----------
#  w4_2m_delta_t   |  1349046
#  w4_2m_pid       |  1348763
#  w4_30m_delta_t  |   180615
#  w4_30m_pid      |   149733
#  w4_150m_delta_t |    77671
#  w4_150m_pid     |    30395
# (6 rows)

# ll /data/mlrepo-backup/

# @DROP_OUTS = qw( pid delta_t );

@DROP_OUTS = qw( link_ctime link_in link_out link_tmo link_tot link_tot_r
                 sched_tcr sched_tde sched_maxinq sched_tlimr sched_jobs_r
                 xrootd_num_r
                 xrootd_ops_getf_r xrootd_ops_putf_r xrootd_ops_misc_r
                 xrootd_ops_rf_r
 );

@PREFS     = qw( w4_2m w4_30m w4_150m );

for my $do (@DROP_OUTS)
{
  print "drop table ", join(', ', map { $_ . "_" . $do } @PREFS), ";\n";
}

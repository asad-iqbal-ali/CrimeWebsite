#!/usr/bin/perl -w
# Program: cass_sample.pl
# Note: includes bug fixes for Net::Async::CassandraCQL 0.11 version

use strict;
use warnings;
use 5.10.0;

use Data::Dumper;
use CGI qw/:standard/, 'Vars';

my $params = Vars;
print header, start_html('hello CGI'), Dumper($params), end_html;


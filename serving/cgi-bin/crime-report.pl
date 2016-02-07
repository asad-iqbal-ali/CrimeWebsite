#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the origin and destination airports as CGI parameters
my $dist = param('dist');
my $month = param('month');

# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
my $hbase = HBase::JSONRest->new(host => "hadoop-m:2056");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

# Query hbase for the route. For example, if the departure airport is ORD
# and the arrival airport is DEN, the "where" clause of the query will
# require the key to equal ORDDEN
my $records = $hbase->get({
  table => 'acidreflux_crime_data',
  where => {
    key_equals => $month.'-'.$dist
  },
});

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row = @$records[0];

# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($year, $arson,$theft,$assault,$battery,$robbery,$burglary,$gambling,$homicide,$stalking,$narcotics,$obscenity,$ritualism,$kidnapping,$sex_offense,$intimidation,$non_criminal,$prostitution,$other_offense,$criminal_damage,$public_indecency,$criminal_trespass,$domestic_violence,$human_trafficking,$weapons_violation,$deceptive_practice,$crim_sexual_assault,$motor_vehicle_theft,$liquor_law_violation,$public_peace_violation,$other_narcotic_violation,$offense_involving_children,$interference_with_public_officer,$concealed_carry_license_violation)
 =  (cellValue($row, 'year:year'),cellValue($row, 'crime:ARSON'),
	cellValue($row, 'crime:THEFT'),cellValue($row, 'crime:ASSAULT'),
	cellValue($row, 'crime:BATTERY'),cellValue($row, 'crime:ROBBERY'),
	cellValue($row, 'crime:BURGLARY'),cellValue($row, 'crime:GAMBLING'),
	cellValue($row, 'crime:HOMICIDE'),cellValue($row, 'crime:STALKING'),
	cellValue($row, 'crime:NARCOTICS'),cellValue($row, 'crime:OBSCENITY'),
	cellValue($row, 'crime:RITUALISM'),cellValue($row, 'crime:KIDNAPPING'),
	cellValue($row, 'crime:SEX_OFFENSE'),cellValue($row, 'crime:INTIMIDATION'),
	cellValue($row, 'crime:NON_CRIMINAL'),cellValue($row, 'crime:PROSTITUTION'),
	cellValue($row, 'crime:OTHER_OFFENSE'),cellValue($row, 'crime:CRIMINAL_DAMAGE'),
	cellValue($row, 'crime:PUBLIC_INDECENCY'),cellValue($row, 'crime:CRIMINAL_TRESPASS'),
	cellValue($row, 'crime:DOMESTIC_VIOLENCE'),cellValue($row, 'crime:HUMAN_TRAFFICKING'),
	cellValue($row, 'crime:WEAPONS_VIOLATION'),cellValue($row, 'crime:DECEPTIVE_PRACTICE'),
	cellValue($row, 'crime:CRIM_SEXUAL_ASSAULT'),cellValue($row, 'crime:MOTOR_VEHICLE_THEFT'),
	cellValue($row, 'crime:LIQUOR_LAW_VIOLATION'),cellValue($row, 'crime:PUBLIC_PEACE_VIOLATION'),
	cellValue($row, 'crime:OTHER_NARCOTIC_VIOLATION'),cellValue($row, 'crime:OFFENSE_INVOLVING_CHILDREN'),
	cellValue($row, 'crime:INTERFERENCE_WITH_PUBLIC_OFFICER'),cellValue($row, 'crime:CONCEALED_CARRY_LICENSE_VIOLATION'));

my @months = qw(January February March April May June July August September October November December);

my $totalyears = $year - 2001;

# Given the number of flights and the total delay, this gives the average delay
sub average_crime {
    my($crimes, $years) = @_;
    return $years > 0 ? sprintf("%.1f", $crimes/$years) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/acidreflux/flights/table.css',-type=>'text/css'}));

print div({-style=>'align:center;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Average Occurrences of Crimes in Ward ' . $dist . ' in ' . @months[$month-1] . '&nbsp;');
print     p({-style=>"bottom-margin:10px"});

print table({-class=>'CSS_Table_Example', -style=>'width:100%;margin:auto;'},
	    Tr([td(['Arson','Theft', 'Assault', 'Battery', 'Robbery', 'Burglary', 'Gambling', 'Homicide',
			'Stalking','Narcotics','Obscenity','Ritualism','Kidnapping','Sex Offense','Intimidation','Non-Criminial',
			'Prostitution', 'Other Offense','Criminal Damage', 'Public Indecency','Criminal Trespass',
			'Domestic Violence','Human Trafficking', 'Weapons Violation','Deceptive Practice','Ciminal Sexual Assault',
			'Motor Vehicle Theft','Liquor Law Violation','Public Peace Violation','Other Narcotic Violation',
			'Offense Involving Children','Interference with Public Officer','Concealed-Carry License Violation']),
               td([ average_crime($arson, $totalyears), average_crime($theft, $totalyears),
                    average_crime($assault, $totalyears), average_crime($battery, $totalyears),
                    average_crime($robbery, $totalyears), average_crime($burglary, $totalyears),
                    average_crime($gambling, $totalyears), average_crime($homicide, $totalyears),
                    average_crime($stalking, $totalyears), average_crime($narcotics, $totalyears),
                    average_crime($obscenity, $totalyears), average_crime($ritualism, $totalyears),
                    average_crime($kidnapping, $totalyears), average_crime($sex_offense, $totalyears),
                    average_crime($intimidation, $totalyears), average_crime($non_criminal, $totalyears),
                    average_crime($prostitution, $totalyears), average_crime($other_offense, $totalyears),
                    average_crime($criminal_damage, $totalyears), average_crime($public_indecency, $totalyears),
                    average_crime($criminal_trespass, $totalyears), average_crime($domestic_violence, $totalyears),
                    average_crime($human_trafficking, $totalyears), average_crime($weapons_violation, $totalyears),
                    average_crime($deceptive_practice, $totalyears), average_crime($crim_sexual_assault, $totalyears),
                    average_crime($motor_vehicle_theft, $totalyears), average_crime($liquor_law_violation, $totalyears),
                    average_crime($public_peace_violation, $totalyears), average_crime($other_narcotic_violation, $totalyears),
                    average_crime($offense_involving_children, $totalyears), average_crime($interference_with_public_officer, $totalyears),
                    average_crime($concealed_carry_license_violation, $totalyears)])])),
	p({-style=>"bottom-margin:10px"})
    ;


print end_html;

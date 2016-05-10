#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: slubsipbuilder.pl
#
#        USAGE: ./slubsipbuilder.pl  
#
#  DESCRIPTION: A CLI tool to create a valid SIP for SLUBArchiv
#
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: not production ready yet
#       AUTHOR: Andreas Romeyke (romeyke@slub-dresden.de)
# ORGANIZATION: SLUB
#      VERSION: 0.1
#      CREATED: 10.05.2016
#===============================================================================

use strict;
use warnings;
use Carp;
use 5.20.0;
use strict;
use warnings;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Data::Printer; # for debugging
use DateTime::Format::ISO8601;
use Digest::MD5 qw(md5);
use File::Basename;
use File::Copy qw(cp);
use File::Find;
use File::Path;
use File::Slurp;
use Getopt::Long;
use LWP::UserAgent; # to get MARC data
use MARC::Record;
use Pod::Usage;
use XML::LibXML;
use XML::LibXSLT;
use XML::XPath;

sub get_mods_from ($$) { # $mods = ($url, $ppn)
    my $url = shift;
    my $ppn = shift; # example: "457035137"  for "Der Fichtelberg"
    #### where to find XSLT
    # my $marc_dc_url = 'http://www.loc.gov/standards/marcxml/xslt/MARC21slim2RDFDC.xsl';
    my $marc_mods_url = 'http://www.loc.gov/standards/mods/v3/MARC21slim2MODS3-5.xsl';

    my $ua = LWP::UserAgent->new; 
    $ua->agent("MyApp/0.1 "); 
    $ua->timeout(3600); #1h
    my $srubase=$url; # host
    my $srusearchkey="pica.ppn";
    my $sruvalue=$ppn;
    my $srumaxrecords=1;
    my $sruschema="marcxml";
    my $sru = "${srubase}?query=${srusearchkey}+%3D+%22${sruvalue}%22&startRecord=1&maximumRecords=${srumaxrecords}&recordSchema=${sruschema}";
    #p ($sru); # debug output
    my $record = $ua->get($sru); # ask SWB for given PPN
    if ($record->is_success) {
        # parse ZiNG repsonse, extract MARC-data
        my $xp = XML::XPath->new( $record->decoded_content );
        my $marcblob = $xp->findnodes_as_string('/ZiNG:searchRetrieveResponse/ZiNG:records/ZiNG:record/ZiNG:recordData/*');
        #p( $marcblob );
        my $parser = XML::LibXML->new();
        my $xslt = XML::LibXSLT->new();
        my $marcmods = XML::LibXML->load_xml(location=>$marc_mods_url, no_cdata=>1);
        # p( $marcmods );
        my $stylesheet = $xslt->parse_stylesheet ( $marcmods);
        my $marc = $parser->parse_string( $marcblob );
        my $result = $stylesheet->transform( $marc);
        return $stylesheet->output_string( $result );
    } else {
        carp ("Problem asking catalogue at $url using $ppn");
    }
}


#===============================================================================

my $directory;
my $ppn;
my $output;
my $url;
my $as_zip;
my $external_id;
my $external_workflow;
my $external_isil;
my $external_value_descr;
my $external_conservation_flag;

our $VERSION = '1.0';
GetOptions(
        "IE_directory=s"            => \$directory,
        "ppn=s"                     => \$ppn,
        "SIP_output_path=s"         => \$output,
        "as_zip"                    => \$as_zip,
        "url=s"                     => \$url,
        "external_id=s"             => \$external_id,
        "external_workflow=s"       => \$external_workflow,
        "external_ISIL=s"           => \$external_isil,
        "external_value_descr=s"    => \$external_value_descr,
        "external_conservation_flag" => \$external_conservation_flag,

        "help"                      => sub { pod2usage(1); exit(0); },
    ) or pod2usage(2);

if (!defined $directory) { confess ("you need to specify an IE directory, which needs to be archived"); }
if (!defined $ppn) { confess ("you need to specify a PPN, which exists in SWB catalogue"); }
if (!defined $output) { confess (" you need to specify an output path, where the SIP will be stored"); }
if (!defined $url) { $url = "http://swb.bsz-bw.de/sru/DB=2.1/username=/password=/";}
if (!defined $external_conservation_flag) { $external_conservation_flag="false"; } else { $external_conservation_flag="true"; } 
# additional checks
if (! -d $directory) { confess("you need to specify an IE directory, which needs to be archived, $!"); }
#if (! -d $output) { confess("you need to specify an output path, where the SIP will be stored, $!"); }

# get date
my $export_to_archive_date = DateTime->now->iso8601();#
# create output dir
mkpath "$output" || confess("could not create SIP directory for '$output', $!");
my $content = "$output/content";
mkpath "$content" || confess("could not create SIP subdirectory for '$content', $!");
# create filecopyhash
my %filecopyhash;
my $wanted=sub {
    if (-d $_) {
        # dir, do nothing
        ()
    } else {
        my $file=$File::Find::name;

        my $source = $file;
        $filecopyhash{$source}->{'source'}=$file;
        $file=~s#^$directory/?##;
        $filecopyhash{$source}{'relative'}=$file;
        $filecopyhash{$source}{'target'}="$content/$file";
        my $fh;
        open($fh, "<", $source) || confess ("Can't open '$source', $!\n");
        binmode($fh);
        my $ctx = Digest::MD5->new;
        $ctx->addfile(*$fh);
        close ($fh);
        my $md5 = $ctx->hexdigest;
        $filecopyhash{$source}{'md5sum'}=$md5;
    }
};
finddepth($wanted, $directory);
p (%filecopyhash);
# prepare dmd-sec
my $mods = get_mods_from($url, $ppn);
my $dmd =<<DMD;
<mets:dmdSec ID="DMDLOG_0000">
  <!-- bibliographic metadata -->
  <mets:mdWrap MDTYPE="MODS">
    <mets:xmlData>
      $mods
    </mets:xmlData>
  </mets:mdWrap>
</mets:dmdSec>
DMD

# prepare amd-sec
my $amd =<<AMD;
<mets:amdSec ID="AMD">
        <!-- SIP metadata for automated processing by submission application -->
        <mets:techMD ID="ARCHIVE">
            <mets:mdWrap MDTYPE="OTHER" MIMETYPE="text/xml" OTHERMDTYPE="ARCHIVE">
                <mets:xmlData>
                    <archive:record xmlns:archive="http://slub-dresden.de">
                        <archive:exportToArchiveDate>$export_to_archive_date</archive:exportToArchiveDate>
                        <archive:externalId>$external_id</archive:externalId>
                        <archive:externalWorkflow>$external_workflow</archive:externalWorkflow>
                        <archive:hasConservationReason>$external_conservation_flag</archive:hasConservationReason>
                        <archive:externalIsilId>$external_isil</archive:externalIsilId>
                        <archive:archivalValueDescription>$external_value_descr</archive:archivalValueDescription>
                    </archive:record>
                </mets:xmlData>
            </mets:mdWrap>
        </mets:techMD>
    </mets:amdSec>
AMD
# create fileSec
my $filesec=<<FILESEC1;
<mets:fileSec>
  <mets:fileGrp USE="LZA">
FILESEC1
my @fsec;
my $i=0;
foreach my $fkey (keys (%filecopyhash)) {
    push @fsec, sprintf("<mets:file ID=\"FILE_%015u_LZA\" CHECKSUMTYPE=\"MD5\" CHECKSUM=\"%s\">", $i, $filecopyhash{$fkey}->{"md5sum"});
    push @fsec, sprintf("<mets:FLocat xmlns:xlink=\"http://www.w3.org/1999/xlink\" LOCTYPE=\"URL\" xlink:href=\"file://%s\"/>", $filecopyhash{$fkey}->{"relative"});
    push @fsec, "</mets:file>";
    $i++;
}
$filesec = join("\n", $filesec, @fsec);
$filesec = $filesec . <<FILESEC2;
  </mets:fileGrp>
</mets:fileSec>
FILESEC2

# create sip.xml
my $sip =<<METS;
<?xml version="1.0" encoding="utf-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-3.xsd http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/version17/mets.v1-7.xsd">
    $dmd
    $amd
    $filesec
</mets:mets>
METS

# compress if needed
if (!defined $as_zip) {
    write_file( "$output/sip.xml", $sip );
    # copy source to target
    foreach my $source (keys (%filecopyhash)) {
        my $target = $filecopyhash{$source}->{"target"};

        my $basename = dirname($target);
        #say "cp $source, $target ($basename)";
        if (! -d $basename) {
            mkpath $basename || confess ("could not mkdir '$basename', $!");
        }
        cp $source, $target || confess ("could not copy from '$source' to '$target', $!");
    }
    say "SIP build successfully in '$output'";
} else {
    # compress it
    my $zip = Archive::Zip->new();
    my $mem = $zip->addString($sip, "sip.xml" );
    $mem->desiredCompressionMethod( COMPRESSION_DEFLATED );
    # copy source to target
    foreach my $source (keys (%filecopyhash)) {
        my $target = $filecopyhash{$source}->{"target"};

        my $basename = dirname($target);
        #say "cp $source, $target ($basename)";
        my $mem = $zip->addFile( $source, $target) || confess ("could not zip copy from '$source' to '$target', $!");
        $mem->desiredCompressionMethod( COMPRESSION_DEFLATED );
    }
    unless ( $zip->writeToFileNamed("$output.zip") == AZ_OK ) {
           confess "write error to $output.zip, $!";
    }
    say "SIP build successfully in '$output.zip'";
}

=pod

=head1 NAME

preingest tool "SIP builder" script to create SIPs for SLUBArchive

=head1 SYNOPSIS

slubsipbuilder.pl  [options]

 Options:
        -help                           brief help message
        -man                            full documentation

        -IE_directory=<IE dir>      	existing IE directory
        -ppn=<ppn>	                    PPN (swb catalogue)
        -SIP_output_path=<target dir>	where to put the SIP dir
        -as_zip                         optional, if set a ZIP will be created
        -url=<SRU url>			        optional, URL of the SRU for PICA catalogues
        -external_id=<id>			    mandatory, should be uniqe ID
        -external_workflow=<workflow>	mandatory, should be uniqe workflow name
        -external_ISIL=<isil>           optional, ISIL number of library
        -external_value_descr=<text>	mandatory, the reason why to archive
        -external_conservation_flag     optional, if set no other "original" still exists

slubsipbuilder.pl --IE_directory=/processdir_from_goobi/10008 --ppn=457035137 --SIP_output_path=/tmp/mysip --external_id=10008 --external_workflow=goobitest --external_ISIL=de-14 --external_value_descr="Gesetzlicher Auftrag" --as_zip

=head1 OPTIONS

=over 8

=item B<-help>

Print a brief help message and exits.

=back

=head1 DESCRIPTION

B<This program> will read  a .gsa file and process all submission directories, check and create SIP directories and ingest them to our
Rosetta (ExLibris) sytem, called SLUBArchive.

=cut
# vim: set tabstop=4

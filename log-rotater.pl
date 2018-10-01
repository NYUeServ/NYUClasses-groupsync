#!/usr/bin/env perl

# Write STDIN to a log file based on a pattern.  If rotation brings us back
# around to a filename we've used previously, we overwrite it.  Intended to
# allow logging a week's worth of stuff without having to explicitly expire old
# logs (i.e. by just logging using day of the week/month names).

use strict;
use POSIX qw(strftime);
use IO::Handle;

sub main {
     my ($pattern) = @ARGV;

     if (!$pattern) {
          die "Usage: $0 <strftime pattern>"
     }

     my $current_output = "";
     my $fh = undef;

     while (my $line = <STDIN>) {
          my $output = strftime($pattern, localtime());

          if ($current_output ne $output) {
               if ($fh) {
                    close($fh);
               }

               $current_output = $output;
               open($fh, ">", $output);
               $fh->autoflush;
          }

          print $fh $line;
     }
}

main();

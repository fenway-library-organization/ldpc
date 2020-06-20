package Biblio::LDP;

use strict;
use warnings;

use DBI;
use POSIX qw(strftime);

sub new {
    my $cls = shift;
    unshift @_, 'name' if @_ % 2;
    my $self = bless { @_ }, $cls;
    $self->init;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub name { @_ > 1 ? $_[0]{'name'} = $_[1] : $_[0]{'name'} }
sub config { @_ > 1 ? $_[0]{'config'} = $_[1] : $_[0]{'config'} }

sub init {
    my ($self) = @_;
    $self->read_config;
    return $self;
}

sub DESTROY {
    my ($self) = @_;
    my $dbh = $self->{'dbh'};
    $dbh->disconnect if $dbh;
}

# --- Command handlers

sub query {
    my ($self, $sql, @params) = @_;
    my $sth = $self->dbh->prepare($sql);
    $sth->execute(@params);
    return $sth if !wantarray;
    return $sth, $sth->{'NAME_lc'}, sub {
        if (my @row = $sth->fetchrow_array) {
            return @row;
        }
    };
}

sub tables {
    my ($self, $filter, %arg) = @_;
    my $dbh = $self->dbh;
    my %want;
    my $test;
    if (!defined $filter) {
        $filter = '%';
    }
    elsif (ref($filter) eq 'Regexp') {
        my $rx = $filter;
        $filter = '%';
        $test = sub { shift()->{'name'} =~ $rx };
    }
    elsif (ref($filter) eq 'CODE') {
        $test = $filter;
        $filter = '%';
    }
    else {
        die "unrecognized table filter: $filter";
    }
    if (defined $arg{'schema'}) {
        $want{'table_schem'} = $arg{'schema'};
    }
    if (defined $arg{'name'}) {
        $want{'table_name'} = $arg{'name'};
    }
    my $schema = $arg{'schema'};
    my $name;
    my $sth = $dbh->table_info(undef, $schema, $filter, 'TABLE');
    my @tables;
TABLE:
    while (my $table = $sth->fetchrow_hashref) {
        ($schema, $name) = delete @$table{qw(table_schem table_name)};
        my $full_name = $schema . '.' . $name;
        my %table = ('schema' => $schema, 'name' => $name, %$table);
        while (my ($k, $v) = each %want) {
            next TABLE if $table{$k} ne $want{$k};
        }
        push @tables, \%table;
    }
    return @tables;
}

sub table {
    # TODO
}

### sub cmd_table {
###     #@ usage: table [SCHEMA] TABLE
###     orient();
### 	usage if @ARGV < 1 || @ARGV > 2;
###     $dbh = dbh($site);
###     my $t = pop @ARGV;
###     my $s = @ARGV ? shift @ARGV : undef;
###     my $sth = $dbh->table_info('', $s, $t, 'TABLE');
###     my $n = 0;
###     while (my $table = $sth->fetchrow_hashref) {
###         print "\n" if ++$n > 1;
###         $table->{'table_name'} = join('.', @$table{qw(table_schem table_name)})
###             if !defined $s;
###         print $table->{'table_name'}, "\n";
###         my $sth_cols = $dbh->column_info(undef, $table->{'table_schem'}, $t, undef);
###         while (my $column = $sth_cols->fetchrow_hashref) {
###             printf "%-32.32s %s\n", @$column{qw(pg_column pg_type)};
###         }
###     }
### }

sub cmd_check {
    subcmd();
}

sub fresh {
    my ($self, %arg) = @_;
    my $t0 = time;
    my $timefile = $arg{'timefile'};
    my $since = $arg{'since'};
    if ($timefile) {
        if (-e $timefile) {
            die "conflicting parameters: fresh('timefile' => FILE, 'since' => TIME)"
                if defined $arg{'since'};
            $since = mtime($timefile);
        }
    }
    my $timestamp;
    if (defined $since) {
        $timestamp = timestamp($since);
    }
    else {
        my $sql = q{
            SELECT  min(all.updated))
            FROM    (
                SELECT max(updated) FROM history.inventory_instances
                UNION ALL
                SELECT max(updated) FROM history.inventory_holdings
                UNION ALL
                SELECT max(updated) FROM history.inventory_items
            ) all
        };
        my $sth = $self->dbh->prepare($sql);
        ($timestamp) = $sth->fetchrow_array;
        $sth->finish;
    }
    my $dbh = $self->dbh;
    my $sth = $dbh->sth(q{
        SELECT bid, max(t) FROM (
            SELECT  id                   AS bid,
                    updated              AS t
            FROM    history.inventory_instances
            WHERE   updated > $1
            UNION
            SELECT  h.instance_id        AS bid,
                    hh.updated           AS t
            FROM    inventory_holdings h INNER JOIN history.inventory_holdings hh ON h.id = hh.id
            WHERE   hh.updated > $1
            UNION
            SELECT  h.instance_id        AS bid,
                    hi.updated           AS t
            FROM    inventory_items i INNER JOIN history.inventory_items hi ON i.id = hi.id INNER JOIN inventory_holdings h ON i.holdings_record_id = h.id
            WHERE   hi.updated > $1
        )
        GROUP BY bid
    });
    $sth->execute($timestamp);
    if (defined $timefile) {
        # Update timestamp
        utime $t0, $t0, $timefile
            or die "touch timestamp file $timefile: $!";
    }
    return $sth if !wantarray;
    return ($sth, sub {
        if (my @row = $sth->fetchrow_array) {
            return wantarray ? @row : $row[0];
        }
    });
}

# --- Other functions

sub atime {
    # time - (-M $f);
    my ($f) = @_;
    my @stat = stat($f);
    return if !@stat;
    return $stat[8];
}

sub mtime {
    # time - (-M $f);
    my ($f) = @_;
    my @stat = stat($f);
    return if !@stat;
    return $stat[9];
}

sub timestamp {
    my $t = pop;  # Allow for $ts = $ldp->timestamp(...) as well as just $ts = timestamp(...)
    return if !defined $t;
    my ($Y, $m, $d, $H, $M, $S, $u, $tzsign, $tz) = (undef, undef, undef, 0, 0, 0, 0, '+', 0);
    if ($t =~ /^[0-9]+$/) {
        ($S, $M, $H, $d, $m, $Y) = gmtime $t;
        $Y += 1900;
        $m++;
    }
    else {
        return if $t !~ s/^([0-9]{4})-?([0-9]{2})-?([0-9]{2})//;
        ($Y, $m, $d) = ($1, $2, $3);
        if ($t =~ s/^[T ]?([0-9]{2}):?([0-9]{2})//) {
            ($H, $M) = ($1, $2);
            if ($t =~ s/^:?([0-9]{2})//) {
                $S = $1;
                if ($t =~ s/^\.([0-9]{6})//) {
                    $u = $1;
                }
            }
        }
        if ($t =~ s/^([-+])([0-9]{2})//) {
            ($tzsign, $tz) = ($1, $2);
        }
        elsif ($t !~ s/^Z?$//) {
            return;
        }
    }
    return sprintf('%04d-%02d-%02d %02d:%02d:%02d.%06d%s%02d', $Y, $m, $d, $H, $M, $S, $u, $tzsign, $tz);
}

sub dbh {
    my ($self) = @_;
    return $self->{'dbh'} if defined $self->{'dbh'};
    my %conn = %{ $self->config };
    my %opt = qw(
        AutoCommit       0
        RaiseError       1
    );
    my @req_args = qw(dbname host port);
    my @opt_args = qw(options service sslmode);
    my @args;
    foreach my $k (@req_args) {
        my $v = $conn{$k} // die "required database connection parameter not configured: $k";
        push @args, $k . '=' . $v;
    }
    foreach my $k (@opt_args) {
        my $v = $conn{$k} // next;
        push @args, $k . '=' . $v;
    }
    my $connstr = 'dbi:Pg:' . join(';', @args);
    my $dbh = DBI->connect($connstr, $conn{'user'}, $conn{'password'}, \%opt);
    $dbh->{'FetchHashKeyName'} = 'NAME_lc';
    # TODO -- error checking
    return $self->{'dbh'} = $dbh;
}

sub read_config {
    my ($self) = @_;
    my %config;
    my $root = $self->root;
    my $f = "$root/ldp.conf";
    open my $fh, '<', $f or die "open $f: $!";
    while (<$fh>) {
        next if /^\s*(?:#.*)?$/;  # Skip blank lines and comments
        chomp;
        /^\s*(\S+)\s*=\s*(.*)$/
            or die "bad config setting at line $. of $f: $_";
        my ($k, $v) = (trim($1), trim($2));
        $config{$k} = $v;
    }
    $self->config(\%config);
    return $self;
}

sub trim {
    local $_ = shift;
    s/^\s+|\s+$//g;
    return $_;
}

# sub norm {
#     local $_ = trim(shift);
#     s/\s+/ /g;
#     return $_;
# }
# 
# sub camelize {
#     local $_ = shift;
#     s/\s+(.)/\U$1/g;
#     return $_;
# }

1;

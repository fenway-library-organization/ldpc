package Biblio::LDP::App;

use strict;
use warnings;

use Biblio::LDP;
use POSIX qw(strftime);
use Getopt::Long
    qw(GetOptionsFromArray :config posix_default gnu_compat require_order bundling no_ignore_case);

sub new {
    my $cls = shift;
    my $self = bless { @_ }, $cls;
    $self->init;
    return $self;
}

sub root { @_ > 1 ? $_[0]{'root'} = $_[1] : $_[0]{'root'} }
sub verbose { @_ > 1 ? $_[0]{'verbose'} = $_[1] : $_[0]{'verbose'} }
sub argv { @_ > 1 ? $_[0]{'argv'} = $_[1] : $_[0]{'argv'} }
sub site_name { @_ > 1 ? $_[0]{'site_name'} = $_[1] : $_[0]{'site_name'} }

sub init {
    my ($self) = @_;
    $self->{'root'} ||= '/usr/local/folio/site';
    # All sites
    $self->{'site_names'} ||= [];
    $self->{'sites'} ||= {};
    # The current site
    $self->{'site_name'} ||= undef;
    $self->{'site'} ||= undef;
    # Run-time params
    $self->{'verbose'} ||= 0;
    $self->{'argv'} ||= \@ARGV;
    return $self;
}

sub run {
    my ($self, %arg) = @_;
    %$self = ( %$self, %arg );
    my $argv = $self->argv;
    $self->usage if @$argv < 1;
    my $site_name;
    if ($argv->[0] =~ /^\@(\S+)$/) {
        $site_name = $1;
        shift @$argv;
    }
    $self->usage if @$argv < 1;
    my $cmd = shift @$argv;
    my $sub = $self->can('cmd_'.$cmd)
        or $self->usage;
    $self->init_sites($site_name);
    $sub->($self);
}

# --- Command handlers

sub cmd_query {
    my ($self) = @_;
    my ($print_header);
    my $argv = $self->orient(
        'h|print-header' => \$print_header,
    );
    $self->usage if !@$argv;
    my $sql = shift @$argv;
    my $site = $self->site;
    my $sth = $site->query($sql);
    $sth->execute(@$argv);
    if ($print_header) {
        print join("\t", @{ $sth->{'NAME_lc'} }), "\n";
    }
    while (my @row = $sth->fetchrow_array) {
        print join("\t", @row), "\n";
    }
}

sub cmd_tables {
    my ($self) = @_;
    my $argv = $self->orient;
    $self->usage if @$argv > 1;
    my $s = @$argv ? shift @$argv : undef;
    my $site = $self->site;
    my $dbh = $site->dbh;
    my $sth = $dbh->table_info(undef, $s, '%', 'TABLE');
    my $n = 0;
    while (my $table = $sth->fetchrow_hashref) {
        $table->{'table_name'} = join('.', @$table{qw(table_schem table_name)})
            if !defined $s;
        my $remarks = $table->{'remarks'} ? " -- $table->{'remarks'}" : '';
        print $table->{'table_name'}, $remarks, "\n";
    }
}

sub cmd_table {
    #@ usage: table [SCHEMA] TABLE
    my ($self) = @_;
    my $argv = $self->orient;
	$self->usage if @$argv < 1 || @$argv > 2;
    my $site = $self->site;
    my $dbh = $site->dbh;
    my $t = pop @$argv;
    my $s = @$argv ? shift @$argv : undef;
    my $sth = $dbh->table_info('', $s, $t, 'TABLE');
    my $n = 0;
    while (my $table = $sth->fetchrow_hashref) {
        print "\n" if ++$n > 1;
        $table->{'table_name'} = join('.', @$table{qw(table_schem table_name)})
            if !defined $s;
        print $table->{'table_name'}, "\n";
        my $sth_cols = $dbh->column_info(undef, $table->{'table_schem'}, $t, undef);
        while (my $column = $sth_cols->fetchrow_hashref) {
            printf "%-32.32s %s\n", @$column{qw(pg_column pg_type)};
        }
    }
}

sub cmd_check {
    subcmd(@_);
}

sub cmd_since {
    my ($self) = @_;
    my $f;
    my $argv = $self->orient(
        'f|timestamp-file=s' => \$f,
    );
    my $timestamp;
    if (defined $f) {
        my $t = mtime($f);
        $timestamp = strftime('%Y-%m-%d %H:%M:%S.000000+00', gmtime $t);
        # $timestamp =~ s/\.000000/sprintf ".%06d", int(($t - int $t) * 1_000_000)/e;
    }
    elsif (@$argv == 1) {
        $timestamp = timestamp(@$argv);
    }
    else {
        $self->usage;
    }
    my $site = $self->site;
    my $sth = $site->query(q{
        SELECT bid, max(t) FROM (
            SELECT  id                   AS bid,
                    updated              AS t
            FROM    history.instances
            WHERE   updated > $1
            UNION
            SELECT  h.instance_id        AS bid,
                    hh.updated           AS t
            FROM    holdings h INNER JOIN history.holdings hh ON h.id = hh.id
            WHERE   hh.updated > $1
            UNION
            SELECT  h.instance_id        AS bid,
                    hi.updated           AS t
            FROM    items i INNER JOIN history.items hi ON i.id = hi.id INNER JOIN holdings h ON i.holdings_record_id = h.id
            WHERE   hi.updated > $1
        )
        GROUP BY bid
    });
    my $t0 = time;
    $sth->execute($timestamp);
    while (my ($bid, $t) = $sth->fetchrow_array) {
        print join("\t", $bid, $t), "\n";
    }
    if (defined $f) {
        # Update timestamp
        my $atime = atime($f);
        utime $atime, $t0, $f
            or $self->fatal("touch timestamp file $f: $!");
    }
}

sub cmd_since_old {
    my ($self) = @_;
    my $argv = $self->orient;
    $self->usage if @$argv != 1;
    my $timestamp = timestamp(@$argv);
    my $site = $self->site;
    my $sth = $site->query(q{
        SELECT  'b'                  AS rectype,
                updated              AS t,
                id                   AS bid,
                NULL                 AS hid,
                NULL                 AS iid
        FROM    history.instances
        WHERE   updated > $1
        UNION
        SELECT  'h'                  AS rectype,
                hh.updated           AS t,
                h.instance_id        AS bid,
                h.id                 AS hid,
                NULL                 AS iid
        FROM    holdings h INNER JOIN history.holdings hh ON h.id = hh.id
        WHERE   hh.updated > $1
        UNION
        SELECT  'i'                  AS rectype,
                hi.updated           AS t,
                h.instance_id        AS bid,
                i.holdings_record_id AS hid,
                i.id                 AS iid
        FROM    items i INNER JOIN history.items hi ON i.id = hi.id INNER JOIN holdings h ON i.holdings_record_id = h.id
        WHERE   hi.updated > $1
        ORDER   BY bid, hid, iid
    });
    $sth->execute($timestamp);
    while (my ($rectype, $t, @ids) = $sth->fetchrow_array) {
        print join("\t", map { defined $_ ? $_ : '' } $rectype, @ids), "\n";
    }
}

sub cmd_check_init {
    # Initialize checkpointing
    my ($self) = @_;
    my $argv = $self->orient;
    my @sql = (
        q{
            CREATE TABLE IF NOT EXISTS local.checkpoints (
                id          SERIAL,
                began       TIMESTAMP WITH TIMEZONE,
                ended       TIMESTAMP WITH TIMEZONE,
                flipflop    INTEGER DEFAULT 0,
                CHECK (flipflop IN (0, 1))
            )
        },
        q{
            CREATE TABLE IF NOT EXISTS local.instances (
                id          VARCHAR(65535),
                createdIn   INTEGER FOREIGN KEY REFERENCES local.checkpoints(id),
                updatedIn   INTEGER FOREIGN KEY REFERENCES local.checkpoints(id),
                deletedIn   INTEGER FOREIGN KEY REFERENCES local.checkpoints(id),
                digest      VARCHAR(64),
                flipflop    INTEGER DEFAULT 0,
                CHECK (flipflop IN (0, 1))
            )
        },
    );
    my $site = $self->site;
    foreach my $sql (@sql) {
        my $sth = $site->query($sql);
        $sth->execute;
    }
}

# --- Other functions

sub subcmd {
    my ($self) = @_;
    my $argv = $self->argv;
    $self->usage if !@$argv;
    my $subcmd = shift @$argv;
    my @caller = caller 1;
    $caller[3] =~ /(cmd_\w+)$/ or die;
    goto &{ __PACKAGE__->can($1.'_'.$subcmd) || $self->usage };
}

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
    my ($t) = @_;
    my ($y, $m, $d, $H, $M, $S, $u, $tzsign, $tz) = (undef, undef, undef, 0, 0, 0, 0, '+', 0);
    return if $t !~ s/^([0-9]{4})-?([0-9]{2})-?([0-9]{2})//;
    ($y, $m, $d) = ($1, $2, $3);
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
    return sprintf('%04d-%02d-%02d %02d:%02d:%02d.%06d%s%02d', $y, $m, $d, $H, $M, $S, $u, $tzsign, $tz);
}

sub dbh {
    my ($self) = @_;
    return $self->{'dbh'} if defined $self->{'dbh'};
    my $site = $self->site
        or $self->fatal("no site");
    my %db = %$site;
    my %opt = qw(
        AutoCommit       0
        RaiseError       1
    );
    my @req_args = qw(dbname host port);
    my @opt_args = qw(options service sslmode);
    my @args;
    foreach my $k (@req_args) {
        my $v = $db{$k} // $self->fatal("required database connection parameter not configured: $k");
        push @args, $k . '=' . $v;
    }
    foreach my $k (@opt_args) {
        my $v = $db{$k} // next;
        push @args, $k . '=' . $v;
    }
    my $connstr = 'dbi:Pg:' . join(';', @args);
    my $dbh = DBI->connect($connstr, $db{'user'}, $db{'password'}, \%opt);
    $dbh->{'FetchHashKeyName'} = 'NAME_lc';
    # TODO -- error checking
    return $self->{'dbh'} = $dbh;
}

sub site {
    my ($self, $s) = @_;
    if (!defined $s) {
        return $self->{'site'} || die "no site specified";
    }
    elsif (ref $s) {
        $self->site_name($s->name);
        return $self->{'site'} = $s;
    }
    else {
        return $self->{'sites'}{$s} ||= Biblio::LDP->new(
            'root' => $self->root . '/site/' . $s,
            'name' => $s,
        );
    }
}

sub init_sites {
    my ($self, $site_name) = @_;
    my $root = $self->root;
    my @site_names;
    foreach my $f (glob("$root/site/*/ldp.conf")) {
        $f =~ m{/site/([^/]+)/ldp\.conf$};
        push @site_names, $1;
    }
    if (defined $site_name) {
        my $site = $self->site($site_name);
        $self->site($site);
    }
    return $self;
}

sub orient {
    my $self = shift;
    GetOptionsFromArray(
        $self->argv,
        'v|verbose' => sub { $self->verbose(1) },
        @_,
    ) or $self->usage;
    return $self->argv;
}

1;

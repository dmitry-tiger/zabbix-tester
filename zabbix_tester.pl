use AnyEvent::DBI::MySQL;
# get cached but not in use $dbh
my $database='zabbix_proxy';
my $hostname='192.168.0.4';
my $port='3306';
my $dbuser='zabbixtester';
my $dbpassword='ZabbixPassw0rd';
my @queue;
my %timetable;
my $limit_rows_from_db=100;
my $timerange=60;
my $dbh = AnyEvent::DBI::MySQL->connect("DBI:mysql:database=$database;host=$hostname;port=$port;",$dbuser,$dbpassword);
my $sel_countq = 'SELECT
        count(*) as count
        FROM
        items , `hosts`
        WHERE
        `hosts`.`status` = 0 AND
        `items`.`status` = 0 AND
        items.hostid = `hosts`.hostid';
$dbh->selectrow_hashref($sel_countq,{async=>0},sub {
    my ($hash_ref) = @_;
    for my $i  (0..int($hash_ref->{count}/$limit_rows_from_db)){
        my $sel_items='SELECT
            items.itemid as itemid,
            items.key_ as key,
            items.delay as delay,
            items.type,
            items.status,
            items.value_type as value_type,
            `hosts`.`status`,
            `hosts`.`host` as host
            FROM
            items , `hosts`
            WHERE
            `items`.`status` = 0 AND
            `hosts`.`status` = 0 AND
            items.hostid = `hosts`.hostid
            LIMIT '.$i*$limit_rows_from_db.','.$limit_rows_from_db;
        $dbh->selectall_hashref($sel_items,'itemid',{async=>0},sub {
            my ($hash_ref) = @_;
           # $timetable
            print scalar keys %{$hash_ref},"\n";
            });
        };
    });

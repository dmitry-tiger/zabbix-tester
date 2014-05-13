use AnyEvent::DBI::MySQL;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use JSON;

my $database='zabbix_proxy';
my $dbhostname='192.168.0.192';
my $dbport='3306';
my $dbuser='zabbixtester';
my $dbpassword='ZabbixPassw0rd';
my $zserverhost= "192.168.0.191";
my $zserverport= '10051';
my $delay_multiplyer = 0.1;
my @queue;
my %timetable;
my $limit_rows_from_db=100;
my $max_send_pack_size=1000;
my $timerange=60;
my $item_sent_counter=0;

my $cv = AnyEvent->condvar;
my $dbh = AnyEvent::DBI::MySQL->connect("DBI:mysql:database=$database;host=$dbhostname;port=$dbport;",$dbuser,$dbpassword);
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
            items.key_ as key_,
            items.delay as delay,
            items.type as itype,
            items.status as istatus,
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
            my $tshift=0;
            foreach my $key (keys %{$hash_ref}){
                push @{$timetable{time()+$tshift}},{itemid=>$hash_ref->{$key}->{itemid},
                                                    key=>$hash_ref->{$key}->{key_},delay=>$hash_ref->{$key}->{delay},
                                                    itype=>$hash_ref->{$key}->{itype},value_type=>$hash_ref->{$key}->{value_type},
                                                    host=>$hash_ref->{$key}->{host}};
                $tshift++;
            }
            });
        };
    });

my $value_generator = AnyEvent->timer (
    after => 0,
    interval => 1,
    cb => sub {
        my $ctime = time();
        if (exists $timetable{$ctime}) {
            #print "exists on ",$ctime,"\n";
            foreach my $kk (@{$timetable{$ctime}}){
                my $val = 0;
                if ($kk->{value_type}==0) {
                    #Numeric float
                    $val = rand(1000);
                }
                elsif ($kk->{value_type}==1){
                    #Character
                    my @chars = ( "A" .. "Z", "a" .. "z", 0 .. 9, " ");
                    $val = join("", @chars[ map { rand @chars } ( 1 .. 12 ) ]);
                }
                elsif ($kk->{value_type}==2){
                    #Log
                    my @chars = ( "A" .. "Z", "a" .. "z", 0 .. 9, " ");
                    $val = join("", @chars[ map { rand @chars } ( 1 .. 50 ) ]);
                }
                elsif ($kk->{value_type}==3){
                    #Numeric (insigned)
                    $val = int(rand(1000));
                }
                elsif ($kk->{value_type}==4){
                    #Text
                    my @chars = ( "A" .. "Z", "a" .. "z", 0 .. 9, " ");
                    $val = join("", @chars[ map { rand @chars } ( 1 .. 100 ) ]);
                }
                my ($clock,$ns)=gettimeofday();
                push @queue,{host=>$kk->{host},key=>$kk->{key},clock=>$clock,ns=>$ns,value=>$val};
                push @{$timetable{$ctime+int($kk->{delay}*$delay_multiplyer)}},$kk;
            }
            delete $timetable{$ctime};
        }
    });

my $value_sender = AnyEvent->timer (
    after => 5,
    interval => 5,
    cb => sub {
        my $pack_size = 0;
        my @pack = ();
        while (@queue) {
            last if ++$pack_size > $max_send_pack_size;
            my $item = shift @queue;
            push @pack,$item;
        }
        my $json = JSON->new();
        my $data = {
        'request' => 'history data',
        'host' => 'test-proxy',
        'data'    => \@pack,
        'clock' => time
        };
        my $json_data = $json->encode($data);
        use bytes;
        my $length = length($json_data);
        no bytes;
        #my $out_data = pack(
        #    "a4 b c4 c4 a*",
        #    "ZBXD", 0x01,
        #    ( $length & 0xFF ),
        #    ( $length & 0x00FF ) >> 8,
        #    ( $length & 0x0000FF ) >> 16,
        #    ( $length & 0x000000FF ) >> 24,
        #    0x00, 0x00, 0x00, 0x00, $json_data
        #);
        my $out_data = pack(
            "a4 b Q a*",
            "ZBXD", 0x01, $length, $json_data
        );

        tcp_connect $zserverhost, $zserverport,
        sub {
            my ($fh) = @_
                or die "unable to connect: $!";
  
            my $handle; 
            $handle = new AnyEvent::Handle
                fh     => $fh,
                on_error => sub {
                AE::log error => $_[2];
                $_[0]->destroy;
            },
            on_eof => sub {
                $handle->destroy; # destroy handle
                AE::log info => "Done.";
            };
  
            $handle->push_write ($out_data);

            $handle->push_read (chunk => 5,  sub {
                my ($response,$d)=unpack ("A4C",$_[1]);
                print "Warn.... Invalid response from Server: \"$response\"\n" if $response ne "ZBXD";
                $handle->on_read (sub {
                shift->unshift_read (chunk => 8, sub {
                    my $len = unpack "Q", $_[1];
                    #print "Length is: $len - unpacked, ".$_[1]."-packed\n";
                    shift->unshift_read (chunk => $len, sub {
                        my $json = decode_json($_[1]);
                        my ($processed_items,$failed_items,$total_items,$time_spent) = $json->{info}=~/^Processed\s+(\d+)\s+Failed\s+(\d+)\s+Total\s+(\d+)\s+Seconds\s+spent\s+([\d\.]+)$/;
                        print "Warn.... Sending failed\n" if $json->{response} eq "failed";
                        print "processed $processed_items, failed $failed_items\n";
                        $item_sent_counter+=$total_items;
                    });
                });
              });  
           });
           
        }, sub {
            my ($fh) = @_;
            # could call $fh->bind etc. here
            #setsockopt($fh, SOL_SOCKET, SO_REUSEADDR, 1)  or die $!;
            
            5  #set timeout
           
        };
    });   
    
my $stat_processing = AnyEvent->timer (
    after => 60,
    interval => 60,
    cb => sub {
       print "processed $item_sent_counter in 1 minute, average speed ".($item_sent_counter/60)." values per second\n";
       $item_sent_counter = 0;
    });
    
      
        
$cv->recv;
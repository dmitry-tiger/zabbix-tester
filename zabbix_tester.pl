use AnyEvent;
use AnyEvent::DBI::MySQL;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Scalar::Util qw(weaken);
use Time::HiRes qw(usleep gettimeofday tv_interval);
use JSON;
use AnyEvent::Run;
use AnyEvent::Monitor::CPU qw( monitor_cpu );

my (@queue,$item_sent_counter,$proxy_id,%timetable,%globalmacro,%hostmacro);
my $proxy_name           = '';
my $database             = 'zabbix';
my $dbhostname           = '';
my $dbport               = '3306';
my $dbuser               = 'zabbix';
my $dbpassword           = '';
my $zserverhost          = "";
my $zserverport          = '10051';
my $stat_server_host     = '';
my $stat_server_port     = '10052';
my $stat_server_stathost = '';
my $work_with_server     = 1;
my $delay_multiplyer     = 0.05;   # Multiplyer value for item delay
my $limit_rows_from_db   = 5000;   # Limit of rows for a query
my $max_send_pack_size   = 1000;  # Max values send in one network session
my $timerange            = 60;    # Period for spreading values in first timetable generation
my $async_mysql_queries  = 1;     # Enable asynq queries in mysql (doesn't work in windows)
my $num_concurent_mysql_threads = 3; # Number of concurent mysql threads
my $num_history_sender_threads = 20; # Number of concurent histry sender threads
my $start_sending_delay  = 180; # Delay for starting send data

my $monitor = monitor_cpu cb => sub {}, interval => 1;

my $cv = AnyEvent->condvar;
my $dbh = AnyEvent::DBI::MySQL->connect("DBI:mysql:database=$database;host=$dbhostname;
                                        port=$dbport;",$dbuser,$dbpassword);

my $sel_globalmacro = 'SELECT macro,value from globalmacro';

# Get all global macro synchoniosly (need get macro before items)
$dbh->selectall_arrayref($sel_globalmacro,{async => 0},sub {
    my ($array_ref) = @_;
    for my $arr_item (@$array_ref){
        $globalmacro{$arr_item->[0]}=$arr_item->[1];
    }
});

my $sel_hostmacro = 'SELECT hostid,macro,value from hostmacro';     

# Get all host macro synchoniosly (need get macro before itema)
$dbh->selectall_arrayref($sel_hostmacro,{async => 0},sub {
    my ($array_ref) = @_;
    for my $arr_item (@$array_ref){
        $hostmacro{$arr_item->[0]}{$arr_item->[1]}=$arr_item->[2];
    }
});

if ($work_with_server) {
    # Get proxy hostid id by proxy name 
    my $sel_hostmacro = 'SELECT hostid FROM `hosts` WHERE `host` = \''.$proxy_name.'\' AND status = 5';     

    $dbh->selectall_arrayref($sel_hostmacro,{async => 0},sub {
        my ($array_ref) = @_;
        if (scalar @$array_ref == 1) {
            for my $arr_item (@$array_ref){
                $proxy_id = $arr_item->[0];
            }
        }
        else{
            die ("Wrong proxy name");
        }

    }); 
}

my $sel_countq = '
    SELECT count(*) AS COUNT
    FROM items ,
         `hosts`
    WHERE `hosts`.`status` = 0
      AND `items`.`status` = 0
      AND items.hostid = `hosts`.hostid
      AND `items`.flags  = 0';
      
# Add filer by proxyid if working with server database
$sel_countq .= " AND `hosts`.proxy_hostid = '$proxy_id'" if $work_with_server;
    
# Get number of items (needed for limiting row number)     
$dbh->selectrow_hashref($sel_countq,{async => $async_mysql_queries},sub {
    my ($hash_ref) = @_;
    
    # Separate queries for get limited row number
    
#    for my $i (0 .. int($hash_ref->{count} / $limit_rows_from_db)){
    our $limit_iterator = 0;
    my $loop;$loop = sub {
        if ($limit_iterator > int($hash_ref->{COUNT} / $limit_rows_from_db)) {
            return;
        }

        # Add filer by proxyid if working with server database
        my $proxy_filter;
        $work_with_server ? ($proxy_filter = " AND `hosts`.proxy_hostid = '$proxy_id'") : ($proxy_filter = "");
        
        print "Selecting with $limit_iterator\n";
        my $sel_items='
            SELECT  items.interfaceid,
                    items.itemid AS itemid,
                    items.key_ AS key_,
                    items.delay AS delay,
                    items.type AS itype,
                    items.status AS istatus,
                    items.value_type AS value_type,
                    `hosts`.`status` AS hstatus,
                    `hosts`.`host` AS hosthost,
                    `hosts`.`name` AS host_name,
                    `hosts`.`hostid` AS hostid,
                    interface.ip AS intip
            FROM items
            JOIN `hosts`
            ON `hosts`.hostid = items.hostid
            LEFT JOIN interface
            ON  items.interfaceid = interface.interfaceid
            WHERE  `items`.`status` = 0
                    AND `hosts`.`status` = 0
                    AND `items`.flags  = 0'.
                    $proxy_filter         
            .' LIMIT '.$limit_iterator*$limit_rows_from_db.','.$limit_rows_from_db;
            $limit_iterator++;
            #Get items from database
            $dbh->selectall_hashref($sel_items, 'itemid', {async => $async_mysql_queries}, sub {
                my ($hash_ref) = @_;
#                print "SQLdone\n";
                # Start after $tshift seconds
                my $tshift=0; 
                
                # Fill timetable with items
                foreach my $key (keys %{$hash_ref}){
                    # Replace internal zabbix macro
                    $hash_ref->{$key}->{key_} =~ s/{HOST\.CONN\d?}/\Q$hash_ref->{$key}->{intip}/g;
                    $hash_ref->{$key}->{key_} =~ s/{HOST\.HOST\d?}/\Q$hash_ref->{$key}->{hosthost}/g;
                    $hash_ref->{$key}->{key_} =~ s/{HOST\.NAME\d?}/\Q$hash_ref->{$key}->{host_name}/g;
                                      
                    # Replace all macro in item
                    if ($hash_ref->{$key}->{key_}=~/\{\$/) {                    
                        # Get all macro in array
                        my @a = $hash_ref->{$key}->{key_} =~ /(\{\$[^\}]+\})/g;
                        
                        # Check and replace macro
                        for my $macro (@a){
                            # Check if host contains hostmacro
                            if (exists $hostmacro{$hash_ref->{$key}->{hostid}}{$macro}) {
                                # DEBUG print "replace $macro to ".$hostmacro{$hash_ref->{$key}->{hostid}}{$macro}."\n";
                                $hash_ref->{$key}->{key_} =~ s/\Q$macro/\Q$hostmacro{$hash_ref->{$key}->{hostid}}{$macro}/g;
                            }
                            
                            # Check if host contains globalmacro
                            if (exists $globalmacro{$macro}) {
                                $hash_ref->{$key}->{key_} =~ s/\Q$macro/\Q$globalmacro{$macro}/g;
                            }
                        }
                    }
                    
                    push @{$timetable{time() + $tshift + $start_sending_delay}}, {
                                            itemid     => $hash_ref->{$key}->{itemid},
                                            key        => $hash_ref->{$key}->{key_},
                                            delay      => $hash_ref->{$key}->{delay},
                                            itype      => $hash_ref->{$key}->{itype},
                                            value_type => $hash_ref->{$key}->{value_type},
                                            hosthost       => $hash_ref->{$key}->{hosthost}
                                            };
                    
                    
                    $tshift < $timerange ? $tshift++ : ( $tshift = 0 );
                }
                print "timetable 1 pass done\n".scalar (keys %timetable)." events created\n";
            $loop->();
	});

        print "Filling timetable done\n"; 
#    };$loop->() for 1 .. $num_concurent_mysql_threads;
    };$loop->();
    weaken($loop);
});

# Timer for check timetable jobs and generate item values
my $value_generator = AnyEvent->timer(
    after => 0,
    interval => 1,
    cb => sub {
        my $ctime = time();
        
        if (exists $timetable{$ctime}) {
            print scalar @{$timetable{$ctime}}." values exists on ",$ctime,"\n";
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
                
                # Get timestamp and nanoseconds
                my ($clock,$ns)=gettimeofday();
                
                if ($kk->{key} eq 'system.localtime'){
                    $val = $clock;
                }
                
                # Add item to send queue
                push @queue,{host=>$kk->{hosthost},key=>$kk->{key},clock=>$clock,ns=>$ns,value=>$val};
#                print "queue length ".scalar @queue."\n";
                
                # Change delay 0 to 60 on trapper items
                $kk->{delay} = 60 if $kk->{delay}==0;
                
                # Readd item to timetable
                push @{$timetable{$ctime+int($kk->{delay}*$delay_multiplyer)}},$kk;
            }
            
            # Remove current timetable jobs
            delete $timetable{$ctime};
        }
    });

# Timer for send items from send queue to zabbix server
#my $value_sender = AnyEvent->timer(
#    after    => 5,
#    interval => 2,
#    cb       => sub {
#        my $pack_size = 0;
#        my @pack = ();
#        
#        # Walk for queue
#        while (@queue) {
#            #Check if pack size less than maximum defined size
#            last if ++$pack_size > $max_send_pack_size;
#            my $item = shift @queue;
#            
#            # Add item to send pack
#            push @pack,$item;
#        }
#        my $json = JSON->new();
#        
#        #Prepare data for sending to zabbix server
#        my $data = {
#        'request' => 'history data',
#        'host'    => 'test-proxy',
#        'data'    => \@pack,
#        'clock'   => time
#        };
#        my $json_data = $json->encode($data);
# 
#        # Get length of data in bytes
#        use bytes;
#        my $length = length($json_data);
#        no bytes;
#        #my $out_data = pack(
#        #    "a4 b c4 c4 a*",
#        #    "ZBXD", 0x01,
#        #    ( $length & 0xFF ),
#        #    ( $length & 0x00FF ) >> 8,
#        #    ( $length & 0x0000FF ) >> 16,
#        #    ( $length & 0x000000FF ) >> 24,
#        #    0x00, 0x00, 0x00, 0x00, $json_data
#        #);
#        
#        # Pack data into zabbix protocol
#        my $out_data = pack(
#            "a4 b Q a*",
#            "ZBXD", 0x01, $length, $json_data
#        );
#        
#        # Create connection to zabbix server
#        tcp_connect $zserverhost, $zserverport,
#        sub {
#            my ($fh) = @_
#                or die "unable to connect: $!";
#  
#            my $handle; 
#            $handle = new AnyEvent::Handle
#                fh     => $fh,
#                on_error => sub {
#                AE::log error => $_[2];
#                $_[0]->destroy;
#            },
#            on_eof => sub {
#                $handle->destroy; # destroy handle
#                AE::log info => "Done.";
#            };
#            
#            # Send data to server
#            $handle->push_write ($out_data);
#            
#            # Read answer (first 5 bytes for check ZBXD header)
#            $handle->push_read (chunk => 5,  sub {
#                my ($response,$d)=unpack ("A4C",$_[1]);
#                print "Warn.... Invalid response from Server: \"$response\"\n" if $response ne "ZBXD";
#                
#                $handle->on_read (sub {
#                # Get length of answer data    
#                shift->unshift_read (chunk => 8, sub {
#                    my $len = unpack "Q", $_[1];
#                    #print "Length is: $len - unpacked, ".$_[1]."-packed\n";
#                    
#                    # Get answer data
#                    shift->unshift_read (chunk => $len, sub {
#                        my $json = decode_json($_[1]);
#                        my ($processed_items,$failed_items,$total_items,$time_spent) = $json->{info}=~/^Processed\s+(\d+)\s+Failed\s+(\d+)\s+Total\s+(\d+)\s+Seconds\s+spent\s+([\d\.]+)$/;
#                        print "Warn.... Sending failed\n" if $json->{response} eq "failed";
#                        print "processed $processed_items, failed $failed_items\n";
#                        $item_sent_counter += $total_items;
#                    });
#                });
#              });  
#           });
#           
#        }, sub {
#            my ($fh) = @_;
#            # could call $fh->bind etc. here
#            #setsockopt($fh, SOL_SOCKET, SO_REUSEADDR, 1)  or die $!;
#            
#            15  #set timeout
#        };
#    });   

# Timer for calculate statistic    
my $stat_processing = AnyEvent->timer (
    after    => 60,
    interval => 60,
    cb       => sub {
        
        my $handle = AnyEvent::Run->new(
            cmd      => [ 'cat', "/proc/$$/statm" ],
            on_eof => sub {
                $_[0]->destroy;
            },
            on_error  => sub {
                $_[0]->destroy;
            },
        );
        
        # Read line from /proc/$$/statm
        $handle->push_read(line => sub {
	    my ($hdl, $line) = @_;
            
            # Split stat for values (man 5 proc for value sequence)
            my @proc_stat = split /\s+/,$line;
            
            
            my $stats = $monitor->stats;
            print "Processed $item_sent_counter in 1 minute, average speed ".($item_sent_counter/60)." values per second\n";
            print "Current send queue size ".($#queue+1).", CPU usage $stats->{usage}, VmSize $proc_stat[0], VmRSS $proc_stat[1]\n";
            my @stat_data = (
                            {host => $stat_server_stathost, key => 'tester1.queue.size',     value => ($#queue+1)},
                            {host => $stat_server_stathost, key => 'tester1.values.sent',    value => ($item_sent_counter/60)},
                            {host => $stat_server_stathost, key => 'tester1.cpu.usage',      value => $stats->{usage}},
                            {host => $stat_server_stathost, key => 'tester1.cpu.avg_usage',  value => ($stats->{usage_avg})},
                            {host => $stat_server_stathost, key => 'tester1.stat.vsize',     value => $proc_stat[0]},
                            {host => $stat_server_stathost, key => 'tester1.stat.rss',       value => $proc_stat[1]},    
                            {host => $stat_server_stathost, key => 'tester1.stat.shared',    value => $proc_stat[2]},
                            {host => $stat_server_stathost, key => 'tester1.stat.datastack', value => $proc_stat[5]}, 
                            
                            );
            zabbix_sender($stat_server_host,$stat_server_port,\@stat_data);
            $item_sent_counter = 0;
            $monitor->reset_stats();
            $handle->destroy;
	});
    });
    
my $sender;$sender = sub {
        my $start = AE::now();
 
        tcp_connect($zserverhost, $zserverport ,sub {
                my ($fh) = @_
                or do {
                    print "unable connect to zabbix server/proxy: $!\n";
                    return $sender->();
#                        die "unable to connect: $!";
                        };
               
#                print "thread started on ".AE::now()."\n";
                my $wait = $start + 2 - AE::now();
                return $sender->() if $wait < 0;
#                do {return $sender->()} if $#queue < 1000;
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
                my $pack_size = 0;
                my @pack = ();
               
                # Walk for queue
                while (@queue) {
                    #Check if pack size less than maximum defined size
                    last if ++$pack_size > $max_send_pack_size;
                    my $item = shift @queue;
                   
                    # Add item to send pack
                    push @pack,$item;
                }
                my $json = JSON->new();
               
                #Prepare data for sending to zabbix server
                my $data = {
                'request' => 'history data',
                'host'    => $proxy_name,
                'data'    => \@pack,
                'clock'   => time
                };
                my $json_data = $json->encode($data);
               
                # Get length of data in bytes
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
               
                # Pack data into zabbix protocol
                my $out_data = pack(
                    "a4 b Q a*",
                    "ZBXD", 0x01, $length, $json_data
                );
 
                $handle->push_write ($out_data);
               
                $handle->push_read (chunk => 5,  sub {
                        my ($response,$d)=unpack ("A4C",$_[1]);
                        print "Warn.... Invalid response from Server: \"$response\"\n" if $response ne "ZBXD";
                       
                        $handle->on_read (sub {
                                # Get length of answer data    
                                shift->unshift_read (chunk => 8, sub {
                                    my $len = unpack "Q", $_[1];
                                    #print "Length is: $len - unpacked, ".$_[1]."-packed\n";
                                   
                                    # Get answer data
                                    shift->unshift_read (chunk => $len, sub {
                                        my $json = decode_json($_[1]);
                                        my ($processed_items,$failed_items,$total_items,$time_spent) = $json->{info}=~/^Processed\s+(\d+)\s+Failed\s+(\d+)\s+Total\s+(\d+)\s+Seconds\s+spent\s+([\d\.]+)$/;
                                        print "Warn.... Sending failed\n" if $json->{response} eq "failed";
#                                        print "Data send: processed $processed_items, failed $failed_items\n";
                                        $item_sent_counter += $total_items;
                                    });
                                });
                        });  
                });
                
                my $t;$t = AE::timer $wait,0, sub {
                        undef $t;
                        $sender->();
                };
        }, sub { 5 })
};$sender->() for 1..$num_history_sender_threads;
#};$sender->();
weaken($sender);    


sub zabbix_sender{
  my ($zabbix_srv_ip, $zabbix_srv_port, $data_pack) = @_;

    my $json = JSON->new();           
    #Prepare data for sending to zabbix server
    my $data = {
    'request' => 'sender data',
    'data'    => \@$data_pack,
    'clock'   => time
    };
    my $json_data = $json->encode($data);
   
    # Get length of data in bytes
    use bytes;
    my $length = length($json_data);
    no bytes;
    tcp_connect($zabbix_srv_ip, $zabbix_srv_port ,sub {
        my ($fh) = @_
        or do {
            print "unable connect to stat host: $!\n";
            return $sender->();
    #                        die "unable to connect: $!";
                };
    
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
        my $out_data = pack(
            "a4 b Q a*",
            "ZBXD", 0x01, $length, $json_data
        );
    
        $handle->push_write ($out_data);
       
        $handle->push_read (chunk => 5,  sub {
                my ($response,$d)=unpack ("A4C",$_[1]);
                print "Warn.... Invalid response from Stat Server: \"$response\"\n" if $response ne "ZBXD";
               
                $handle->on_read (sub {
                        # Get length of answer data    
                        shift->unshift_read (chunk => 8, sub {
                            my $len = unpack "Q", $_[1];
                            #print "Length is: $len - unpacked, ".$_[1]."-packed\n";
                           
                            # Get answer data
                            shift->unshift_read (chunk => $len, sub {
                                my $json = decode_json($_[1]);
                                my ($processed_items,$failed_items,$total_items,$time_spent) = $json->{info}=~/^Processed\s+(\d+)\s+Failed\s+(\d+)\s+Total\s+(\d+)\s+Seconds\s+spent\s+([\d\.]+)$/;
                                print "Warn.... Sending failed\n" if $json->{response} eq "failed";
                                print "Stat send: processed $processed_items, failed $failed_items\n";
                            });
                        });
                });  
        });
    
    }, sub { 5 });
}    
    
    
    
# signal handlers take function name
# instead of being references to functions
sub unloop { $cv->send; }
$SIG{INT} = 'unloop';
    
# start waiting in event loop    
$cv->recv;
print "Done";
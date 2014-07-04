#!/usr/bin/perl

my %testers = (
    tester1 => {
               tester_identifier => 'tester1',
               proxy_name => 'test-proxy1',

               zserverhost => '10.1.1.11',
               zserverport => '10051',

               delay_multiplyer => 0.5,

               stat_server_host => '10.1.1.5',
               stat_server_port => '10051',
               stat_server_stathost => 'zabbix-test1',

               dbhostname => '10.1.1.11',
               database => 'zabbix',
               dbuser => 'zabbix',
               dbpassword => 'my_db_password',
               },
    tester2 => {
               tester_identifier => 'tester2',
               proxy_name => 'test-proxy2',

               zserverhost => '10.1.1.11',
               zserverport => '10051',

               delay_multiplyer => 0.1,

               stat_server_host => '10.1.1.5',
               stat_server_port => '10051',
               stat_server_stathost => 'zabbix-test1',

               dbhostname => '10.1.1.11',
               database => 'zabbix',
               dbuser => 'zabbix',
               dbpassword => 'my_db_password',
               },
);

my %instances = ();

# Generate script params
for my $key (keys %testers){
    my $args = '';
    for my $param (keys %{$testers{$key}}){
        $args.=" --$param=\"$testers{$key}{$param}\"";
    }
    $instances{$key}=$args;
}

sub check_if_started($){
    my $inst = shift;
    my $a=qx#ps ax | grep -E '(SCREEN|screen).*$inst' | grep -v grep | awk '{print \$1}'#;
    $a ? return $a : return 0;    
}

sub run_inst($){
    my $inst = shift;
    my $a=qx#screen -amdS $inst perl zabbix_tester.pl $instances{$inst}#;
    print "started $inst with screen -amdS $inst perl zabbix_tester.pl $instances{$inst}  res=$a\n";
}

if (!$ARGV[0]){
    print "Use with start|stop|status args\n";
    exit 1;
}

if ($ARGV[0] eq 'start') {
    for my $inst (keys %instances){
        my $a;
        if ($a=check_if_started($inst)){
            print "process $inst started... skip \n";
        }
        else {
            run_inst($inst);
        }
    }
    
}

if ($ARGV[0] eq 'restart') {
        for my $inst (keys %instances){
        my $a;
        if ($a=check_if_started($inst)){
            print "process $inst started with pid $a... killing \n";
            `kill $a`;
            run_inst($inst);
        }
        else {
            print "process $inst already stopped\n";
            run_inst($inst);
        }
    }
}

if ($ARGV[0] eq 'stop') {
        for my $inst (keys %instances){
        my $a;
        if ($a=check_if_started($inst)){
            print "process $inst started with pid $a... killing \n";
            `kill $a`;
        }
        else {
            print "process $inst already stopped\n";
        }
    }
}

if ($ARGV[0] eq 'status') {
        for my $inst (keys %instances){
        my $a;
        if ($a=check_if_started($inst)){
            print "process $inst started with pid $a\n";
        }
        else {
            print "process $inst not running\n";
        }
    }
}

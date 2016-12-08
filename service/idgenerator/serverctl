#!/bin/bash
rw=`dirname $0` 
cd $rw 
ROOT_DIR=`pwd`
APP=`basename $ROOT_DIR`

wait_for_pid () {
        try=0
        #先sleep1秒, 防止启动后马上又出错退出的情况
        sleep 1
        while test $try -lt 15 ; do

                case "$1" in
                        'created')
                        if [ -f "$2" ] ; then
                                return 0
                        fi
                        ;;

                        'removed')
                        if [ ! -f "$2" ] ; then
                              return 0
                        fi
                        ;;
                esac

                echo -n .
                try=`expr $try + 1`
                sleep 1

        done
        return 1
}

case "$1" in
        start)
               echo "starting...."
               rm -rf $ROOT_DIR/bin/pid
               if [ -s $ROOT_DIR/bin/pid ]
               then
                     #强制启动，会忽略pid文件的存在， 适合程序异常退出后的重启，或者机器重启的场景
                     if [ "$2" = "-f" ]
                     then
                        echo "ignore existed pid file"
                        rm -rf $ROOT_DIR/bin/pid
                     else
                        echo "pid file already exist"
                        exit 1
                     fi
               fi
               export GOGC=200
               nohup $ROOT_DIR/bin/$APP -f $ROOT_DIR/conf/idgenerator.ini  1>$ROOT_DIR/logs/run.log 2>$ROOT_DIR/logs/run.log &
               wait_for_pid created $ROOT_DIR/bin/pid
               if [ 0 != $? ] 
               then
                        echo "failed, please refer to logs/run.log for more detail"
                        exit 1
               else
                        echo "done"
               fi
        ;;
        
        stop)
                echo "stopping...."
                kill -9 `cat $ROOT_DIR/bin/pid`
                if [ 0 != $? ]
                then
                    echo "failed"
                    exit 1
                else
                    rm -rf $ROOT_DIR/bin/pid
                    echo "done"
                fi
        ;;

        restart)
                sh $0 stop
                echo "To start session in 2 seconds later..."
                sleep 2
                sh $0 start
                if [ $? != 0 ]
                then
                    echo "failed"
                    exit 1
                fi     
        ;;

        reload)
                #todo热启动
                echo "not supported yet...."
                #kill -HUP `cat $ROOT_DIR/bin/pid`
                echo "done"
        ;;
        envinit)
                chmod -R 0777 $ROOT_DIR
                chmod 0777 $ROOT_DIR/bin
                #if test $# -lt 2
                #then
                #    echo Usage: $0 envinit idc
                 #   echo    eg: $0 envinit zwt
                 #   exit 1
                #fi
                REGION=`hostname | awk -F. '{ print $3 }'`
                DIRS="logs"
                EXECUTES=""
             
                cd $ROOT_DIR/conf
                if test -e app.conf
                then 
                    rm -rf app.conf
                fi
                if (test -s app.conf.$REGION)
                then
                    ln -s app.conf.$REGION app.conf
                    echo link -s app.conf.$REGION ........... OK
                else 
                    echo link -s app.conf.$REGION  ........... Fail
                fi
                
                cd $ROOT_DIR
                for dir in $DIRS
                do
                    if (test ! -d $dir)
                    then
                        mkdir -p $dir
                    fi
                    chmod 0777 $dir
                    echo mkdir $dir ................ OK
                done
                for execute in $EXECUTES
                do
                    sh $execute > /dev/null
                    if test $? -eq 0
                    then
                        echo sh $execute ................ OK
                    fi
                done   
        ;;
        *)
                echo "Usage: $0 {start [-f]|stop|restart|envinit}"
                exit 1
                
        ;;
esac

exit 0


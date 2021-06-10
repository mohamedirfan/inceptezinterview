#set -x
dt=`date '+%Y%m%d%H'`

if [ $# -ne 1 ]
then
echo "missing the expected arguments, usage: bash sfm_insuredata.sh https://s3.amazonaws.com/in.inceptez.bucket1/insurance_project/insuranceinfo.csv"
exit 0
fi

if [ -d /tmp/clouddata ]
then
echo "src dir is present"
else
mkdir /tmp/clouddata
fi

if [ -d /tmp/clouddata/archive ]
then
echo "archival path exists"
else
mkdir /tmp/clouddata/archive
fi


echo "`date` importing data from cloud" >> /tmp/sfm_${dt}.log
wget $1 -O /tmp/clouddata/creditcard_insurance
#wget https://s3.amazonaws.com/in.inceptez.bucket1/insurance_project/insuranceinfo.csv -O /tmp/clouddata/creditcard_insurance

if [ $? -eq 0 ]
then
echo "`date` import of data from cloud is completed"
echo "import of data from cloud is completed" >> /tmp/sfm_${dt}.log
else
echo "no data imported from cloud"
exit 0
fi

if [ -f /tmp/clouddata/creditcard_insurance ]
then
 echo "`date` file is present, proceeding further" >> /tmp/sfm_insurance${dt}.log
 mv /tmp/clouddata/creditcard_insurance /tmp/clouddata/creditcard_insurance_${dt}
 trlcnt=`tail -1 /tmp/clouddata/creditcard_insurance_${dt} | awk -F'|' '{ print $2 }'`
 filecnt=`cat /tmp/clouddata/creditcard_insurance_${dt} | wc -l`
 echo "trailer count is $trlcnt"
 echo "file count is $filecnt"
 if [ $trlcnt -ne $filecnt ]
 then
 echo "`date` moving to reject, file is invalid" >> /tmp/sfm_${dt}.log
 mkdir -p /tmp/clouddata/reject
 mv /tmp/clouddata/creditcard_insurance_${dt} /tmp/clouddata/reject/
 exit 0
 fi
 echo "`date` Remove the trailer line in the file"
 sed -i '$d' /tmp/clouddata/creditcard_insurance_${dt}
 hadoop fs -mkdir -p /user/hduser/insurance_clouddata/

 #Check whether the above dir is created in Hadoop
 hadoop fs -test -d /user/hduser/insurance_clouddata

 if [ $? -eq 0 ]
 then
 echo "`date` hadoop directory is created " >> /tmp/sfm_${dt}.log
 else
 echo "`date`  failed to create the hadoop directory /user/hduser/clouddata " >> /tmp/sfm_${dt}.log
 exit 0
 fi

 hadoop fs -D dfs.block.size=67108864 -copyFromLocal -f /tmp/clouddata/creditcard_insurance_${dt} /user/hduser/insurance_clouddata/
 if [ $? -eq 0 ]
 then
 hadoop fs -touchz /user/hduser/insurance_clouddata/_SUCCESS
 echo "Data copied to HDFS successfully"
 echo "`date` Data copied to HDFS successfully" >>  /tmp/sfm_${dt}.log
 else
 echo "Failed to copy data to HDFS `date` " >> /tmp/sfm_${dt}.log
 fi
 echo "`date` moving to linux archive after compressing" >> /tmp/sfm_${dt}.log
 gzip /tmp/clouddata/creditcard_insurance_${dt}
 mv /tmp/clouddata/creditcard_insurance_${dt}.gz /tmp/clouddata/archive/
else
 echo "No data to process"
 echo "`date` No data to process" >>  /tmp/sfm_${dt}.log
 exit 0
fi

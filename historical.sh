# !/bin/bash
# To declare static Array

. /usr/apps/nsp/stp/config/stpconfig.cfg

arr=(2019-10-01 2019-10-02 2019-10-03 2019-10-04 2019-10-05 2019-10-06 2019-10-07 2019-10-08 2019-10-09 2019-10-10 2019-10-11 2019-10-12 2019-10-13 2019-10-14 2019-10-15 2019-10-16 2019-10-17 2019-10-18 2019-10-19 2019-10-20 2019-10-21 2019-10-22 2019-10-23 2019-10-24 2019-10-25 2019-10-26 2019-10-27 2019-10-28 2019-10-29 2019-10-30 2019-10-31 2019-11-01 2019-11-02 2019-11-03 2019-11-04 2019-11-05 2019-11-06 2019-11-07 2019-11-08 2019-11-09 2019-11-10 2019-11-11 2019-11-12 2019-11-13 2019-11-14 2019-11-15 2019-11-16 2019-11-17 2019-11-18 )
i=0

# Loop upto size of array
# starting from index, i=0
while [ $i -lt ${#arr[@]} ]
do
  # To print index, ith
  # element
  exec_date=${arr[$i]}
  #echo $exec_date
  sh sh.sh $exec_date
  # Increment the i = i + 1
i=`expr $i + 1`
done

run commands and both the processes are independent
sh -vx stp_customer_event_history_new_site_build.sh 2019-12-13

sh -vx stp_new_site_build_scores.sh 2019-12-13 


Scripts to execute  - Customer_event_history_new_site:

# !/bin/bash
# To declare static Array

. /usr/apps/nsp/stp/config/stpconfig.cfg

arr=(2019-10-01 2019-10-02 2019-10-03 2019-10-04 2019-10-05 2019-10-06 2019-10-07 2019-10-08 2019-10-09 2019-10-10 2019-10-11 2019-10-12 2019-10-13 2019-10-14 2019-10-15 2019-10-16 2019-10-17 2019-10-18 2019-10-19 2019-10-20 2019-10-21 2019-10-22 2019-10-23 2019-10-24 2019-10-25 2019-10-26 2019-10-27 2019-10-28 2019-10-29 2019-10-30 2019-10-31 2019-11-01 2019-11-02 2019-11-03 2019-11-04 2019-11-05 2019-11-06 2019-11-07 2019-11-08 2019-11-09 2019-11-10 2019-11-11 2019-11-12 2019-11-13 2019-11-14 2019-11-15 2019-11-16 2019-11-17 2019-11-18)
i=0

# Loop upto size of array
# starting from index, i=0
while [ $i -lt ${#arr[@]} ]
do
  # To print index, ith
  # element
  exec_date=${arr[$i]}
  echo $exec_date
  sh -vx stp_customer_event_history_new_site_build.sh $exec_date
  RET_VAL=$?
	if [ $RET_VAL -eq 0 ]
	then
		echo "CUSTOMER EVENT HISTORY was successfully completed for  $exec_date " 
	else
	echo "CUSTOMER EVENT HISTORY was failed for  $exec_date " 
		  exit 1
	fi
  # Increment the i = i + 1
i=`expr $i + 1`
done



Scripts to execute  - Customer_event_history_build_site:


arr=(2019-10-01 2019-10-02 2019-10-03 2019-10-04 2019-10-05 2019-10-06 2019-10-07 2019-10-08 2019-10-09 2019-10-10 2019-10-11 2019-10-12 2019-10-13 2019-10-14 2019-10-15 2019-10-16 2019-10-17 2019-10-18 2019-10-19 2019-10-20 2019-10-21 2019-10-22 2019-10-23 2019-10-24 2019-10-25 2019-10-26 2019-10-27 2019-10-28 2019-10-29 2019-10-30 2019-10-31 2019-11-01 2019-11-02 2019-11-03 2019-11-04 2019-11-05 2019-11-06 2019-11-07 2019-11-08 2019-11-09 2019-11-10 2019-11-11 2019-11-12 2019-11-13 2019-11-14 2019-11-15 2019-11-16 2019-11-17 2019-11-18)
i=0

# Loop upto size of array
# starting from index, i=0
while [ $i -lt ${#arr[@]} ]
do
  # To print index, ith
  # element
  exec_date=${arr[$i]}
  echo $exec_date
  sh -vx stp_new_site_build_scores.sh $exec_date
  RET_VAL=$?
	if [ $RET_VAL -eq 0 ]
	then
		echo "CUSTOMER EVENT HISTORY was successfully completed for  $exec_date " 
	else
	echo "CUSTOMER EVENT HISTORY was failed for  $exec_date " 
		  exit 1
	fi
  # Increment the i = i + 1
i=`expr $i + 1`
done

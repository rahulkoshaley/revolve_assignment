download spark from -> https://spark.apache.org/downloads.html

$ tar -xvf spark-3.1.1-bin-hadoop2.7
$ sudo mv spark-3.1.1-bin-hadoop2.7 /opt/spark
$ cd /opt/spark


$cd /opt/spark/conf

$ cp spark-env.sh.template spark-env.sh

$ sudo ~/.bashrc
$ source ~/.bashrc

copy at last these 3 lines

export SPARK_HOME=/opt/spark

export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

export PYSPARK_PYTHON=/usr/bin/python3.7

Activate tne Environment - 

1) Using virtualenv
    step1 - pip3 install virtualenv
    step2 - python3.7 -m virtualenv MyEnv
    step3 - source MyEnv/bin/activate

OR

2) Using conda

$ conda actiavte env1 



pip install pyspark
pip install pytest
pip install findspark
pip install unittest-pyspark


The code instructions are given in the jupyter notebook evolve.ipnyb and unittest.ipnyb file

After downloading the data and jupyter notebooks , only change the file paths as per your computer and run the ipynb files

Also provided all the .py files



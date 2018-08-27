import os
import sys

if(len(sys.argv)<2): 
    print("ERROR: Use Python3 rerunspark.py <simulation_id>. <simulation_id> is required")
    exit(0)

lastS = int(sys.argv[1])

os.system('ssh bigdatadb export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/')
print (1)

os.system('scp ../Analysis/Spark_Analyzer.py bigdatadb:/tmp/CarSharing_Spark_Analyzer.py')
print(2)

os.system('ssh bigdatadb spark2-submit --master yarn --deploy-mode client /tmp/CarSharing_Spark_Analyzer.py %d'%lastS)
print(3)

os.system('scp bigdatadb:/data/03/Carsharing_data/output_analysis/Simulation_%s/out_analysis.txt ../output_analysis/Simulation_%d/'%(lastS,lastS))
print(4)
#os.system('python3 ../Analysis/plot_heatmap.py %d'%lastS)



#os.system('cat ../output_analysis/Simulation_%d/out_analysis.txt | ssh bigdatadb hdfs dfs -put -f - Simulator/output/Simulation_%s/out_analysis.txt' %(lastS,lastS))



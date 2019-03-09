
Q1)
EXECUTION:
hadoop jar <jar file path> <class name> <input file path> <output file path>

asdf@asdf-VirtualBox:~$ hadoop jar /home/asdf/IdeaProjects/MutualFriends/out/artifacts/MutualFriends_jar/MutualFriends.jar MutualFriends /user/asdf/input/soc-LiveJournal1Adj.txt /user/asdf/out1

To find a pair:
eg: 0,1
asdf@asdf-VirtualBox:~$ grep -w "0,1" /home/asdf/out1/part-r-00000

Q2)
EXECUTION:
hadoop jar <jar file path> <class name> <path of mutual friends file> <output path>

asdf@asdf-VirtualBox:~$ hadoop jar /home/asdf/IdeaProjects/MutualFriends2/out/artifacts/MutualFriends2_jar/MutualFriends2.jar MutualFriends2 /user/asdf/input/MFs /user/asdf/out2

Q3)
EXECUTION:
hadoop jar <jar file path> <class name> <personA> <personB> <path of userdata.txt> <path of mutual friends file> <output path>

asdf@asdf-VirtualBox:~$ hadoop jar /home/asdf/IdeaProjects/MutualFriends3/out/artifacts/MutualFriends3_jar/MutualFriends3.jar MutualFriends3 0 10 /user/asdf/input/userdata.txt /user/asdf/input/MFs /user/asdf/out3

Q4)
EXECUTION:
hadoop jar <jar file path> <class name> <path of userdata.txt> <path of direct friends file> <intermediate file path> <output file path>

hadoop jar /home/asdf/IdeaProjects/MutualFriends4/out/artifacts/MutualFriends4_jar/MutualFriends4.jar MutualFriends4 /user/asdf/input/userdata.txt /user/asdf/input/soc-LiveJournal1Adj.txt /user/asdf/intr /user/asdf/output4

asdf@asdf-VirtualBox:~$ hadoop fs -cat /user/asdf/output4/part-r-00000 | tail -15



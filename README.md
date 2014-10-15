DSProj3
See PDF in the project
=======

MapReduce


====
reference : 

framework
https://github.com/lzl1024/MapReduce

file splitter
http://www.eecis.udel.edu/~trnka/CISC370-05J/code/FileSplitter/FileSplitter.java


====

TODO:

1. Socket will closed when transfering large file. √

2. Socket will only receive 2894 bytes at most. √

3. Charactertics could be splitted in the middle of a word - won't do

===

Recovery:

1. slave info add status
2. 
2. check file layout and slavepool’ s slave info are same or not
3. 
3. if one node down, change status of slave info in slavepool and file layout
4. 
4. slave keeps thread info in case to kill them
5. 

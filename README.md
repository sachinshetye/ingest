### How to run the program

 ```
 git clone the repository
 Open App.java in an IDE (tested on vscode, intellij)
 Update lines 17, 18, 19 with inputPath, changesPath, outputPath
 
 NOTE changes.json needs to be in the json format as supplied 
 
 mvn clean install
 Open App.java in an IDE (tested on vscode, intellij) 
 Run the main program
 
 O/P of a successful run
 
 /Library/Java/JavaVirtualMachines/jdk-11.0.1.jdk/Contents/Home/bin/java -javaagent:/Applications/IntelliJ IDEA 2.app/Contents/lib/idea_rt.jar=51274:/Applications/IntelliJ IDEA 2.app/Contents/bin -Dfile.encoding=UTF-8 -classpath /Users/sshetye/Desktop/highspot/ingest/target/classes:/Users/sshetye/.m2/repository/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar batch.App
Task performed by Available Worker Node : 4
Creating playlist : 4
Task performed by Available Worker Node : 0
Creating playlist : 5
Task performed by Available Worker Node : 1
Updating playlist : 1
Task performed by Available Worker Node : 4
Updating playlist : 4
Task performed by Available Worker Node : 0
Removing playlist : 3
Task performed by Available Worker Node : 1
Removing playlist : 2

Process finished with exit code 0   
 ```

### Assumptions

1. changes.json data is validated for correctness. i.e. the songs, playlists data it contains are valid.
If changes.json contains invalid data then a validation function can be added at line 29, 30 in App.java

2. The changes.json can contain multiple tasks that refer to the same playlist. ex: add songs to a playlist multiple times. 
For the purpose of this exercise, I assume that each playlist is referred to only once in changes.json.
   
3. NOTE: The output of the program is not pretty formatted. It is still valid json.
Due to time constraints, I did not have the chance to switch to another package for pretty formatting output.

### Scalability

The application is designed in a manner, that it captures in essence how scalability can be achieved.

* For large input mixtape.json
  
The input data for each of the tables (users, playlists, songs) may not fit on a single machine.
In order to scale horizontally, the three tables are stored on different nodes.
A consistent hashing also known as distributed hash table (DHT) scheme can be applied to distribute the read/write load across nodes.
ex: user data could reside on 2 nodes, playlists on 6 nodes, songs on 10 nodes.
The data should be stored in at least two nodes to add redundancy.  
By having different nodes manage different tables, the tables can scale independently of each other.  

* For large changes.json

The changes to be applied can be thought of as a task list. All the tasks can be added to a queue.
There can be few worker nodes that take ownership of the tasks in queue and execute them.
Depending on the requirements, we can either implement a pull or push model for task allocation to workers.
If the task list is significantly large, by adding more worker nodes a.k.a autoscaling we can achieve horizontal scaling. 



# ingest

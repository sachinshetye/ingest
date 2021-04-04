package batch;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;

import java.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class App 
{
    public static void main(String args[])
    {
        String inputPath = "/Users/sshetye/Desktop/highspot/ingest/src/mixtape.json";
        String changesPath = "/Users/sshetye/Desktop/highspot/ingest/src/changes.json";
        String outputPath = "/Users/sshetye/Desktop/highspot/ingest/src/result.json";
        Object input = readJSONFile(inputPath);
        Object changes = readJSONFile(changesPath);
        
        // User/PlayList/SongNodes can be thought of generic nodes used for horizontal scaling
        // with DHT-Distributed Hash Table aka Consistent Hashing
        ArrayList<ArrayList<JSONObject> > UserNodes = initNodes("users", "name", "string", input);
        ArrayList<ArrayList<JSONObject> > PlayListNodes = initNodes("playlists", "id", "int", input);
        ArrayList<ArrayList<JSONObject> > SongNodes = initNodes("songs", "id", "int", input);

        // ASSUMPTION: changes are pre-validated for correctness
        // Optional: changes can be validated using the User/PlayList/SongNodes
        
        // Store tasks in the queue on the node
        Queue<Object> taskQueue = getTaskQueue();
        addChangesToTaskQueue(taskQueue, changes);
        
        int WORKER_COUNT = 5; 
        processTasksFromQueue(taskQueue, PlayListNodes, createWorkers(WORKER_COUNT));

        try {
            writeOutputData(UserNodes, PlayListNodes, SongNodes, outputPath); 
        } catch (FileNotFoundException ex) {
            System.out.print("Failure to write output file " + ex);
        }
    }
    
    static class Worker {
        public
         State getState() {return state; }
         int getId() {return id; }
         void setState(Worker.State newState) { state = newState;}
         Worker(int wid, State initial_state) { 
            id = wid;
            state = initial_state;}

        enum State {
            AVAILABLE,
            WORKING,
            OUT_OF_SERVICE
        }
        private
         int id;
         State state;
    }

    private static Vector<Worker> createWorkers(int count) {
        Vector<Worker> workers = new Vector<>();
        for (int i = 0; i < count; ++i) {
            if (i%3 == 0) {
                workers.add(new Worker(i, Worker.State.AVAILABLE));
            } else if (i%3 == 1) {
                workers.add(new Worker(i, Worker.State.WORKING));
            } else if (i%3 == 2) {
                workers.add(new Worker(i, Worker.State.OUT_OF_SERVICE));
            }
        }
        return workers;
    }

    private static void addChangesToTaskQueue(Queue<Object> q, Object data) {
        JSONObject jsonObject = (JSONObject)data;
        JSONArray entries = (JSONArray)jsonObject.get("tasks");
        for(Object entry : entries)
        {
            JSONObject jEntry= (JSONObject)entry;
            q.add(jEntry);
        }   
    }

    private static void processTasksFromQueue(Queue<Object> taskQueue, 
        ArrayList<ArrayList<JSONObject> > PlayListNodes,
        Vector<Worker> workers) {

         // while there are tasks in queue
         while (!taskQueue.isEmpty()) {
            // find a available node
            Worker worker = findNode(workers, Worker.State.AVAILABLE); 
            // update state of all workers
            updateAllWorkerStates(workers, worker);

            // assign task to an available worker
            if (worker != null) {
                performTask(worker, taskQueue.remove(), PlayListNodes);
            }
         }       
    }
    
    private static void performTask(Worker worker, Object newObj, ArrayList<ArrayList<JSONObject> > PlayListNodes) {
        JSONObject jObject = (JSONObject) newObj;
        // System.out.println(jObject);
        String operation = (String)jObject.get("operation");
        String playlistID = (String)jObject.get("id");;  

        // worker locates node by lookup
        int nodeId = getNodeToManageForInt(playlistID);
        ArrayList<JSONObject> playlists = PlayListNodes.get(nodeId);

        if (operation.equals("delete_pl")) {
            for (JSONObject playlist: playlists) {
                if (playlist.get("id").equals(playlistID)) {
                    System.out.println("Removing playlist : " + playlistID);
                    playlists.remove(playlist);
                    break;
                }
            }
        } else if (operation.equals("create_pl")) {
            System.out.println("Creating playlist : " + playlistID);
            jObject.remove("operation");
            playlists.add(jObject);

        } else if (operation.equals("update_pl")) {
            for (JSONObject playlist: playlists) {
                if (playlist.get("id").equals(playlistID)) {
                    System.out.println("Updating playlist : " + playlistID);
                    JSONArray songs = (JSONArray)playlist.get("song_ids");
                    JSONArray newSongs = (JSONArray)jObject.get("song_ids");
                    for(Object newSong: newSongs) {
                        songs.add((String)newSong);
                    }
                    playlist.put("song_ids", songs);
                    break;
                }
            }        
        }
    }

    private static Worker findNode(Vector<Worker> workers, Worker.State state) {
        Random random = new Random(); 
        int pos = random.nextInt(5);  
        for (;pos < workers.size(); ++pos) {
            if (workers.elementAt(pos).getState() == state) {
                System.out.println("Task performed by Available Worker Node : " + pos);
                return workers.elementAt(pos);
            }    
        }
        return null;
    }

    private static void updateAllWorkerStates(Vector<Worker> workers, Worker availableWorker) {
        for (int pos = 0; pos < workers.size(); ++pos) { 
            if (availableWorker != null && 
                workers.elementAt(pos).getId() == availableWorker.getId()) {
                workers.elementAt(pos).setState(Worker.State.WORKING);
            } else if (workers.elementAt(pos).getState() == Worker.State.WORKING) {
                workers.elementAt(pos).setState(Worker.State.OUT_OF_SERVICE);
            } else if (workers.elementAt(pos).getState() == Worker.State.OUT_OF_SERVICE) {
                workers.elementAt(pos).setState(Worker.State.AVAILABLE);
            }
        }    
    }

    private static void writeOutputData(ArrayList<ArrayList<JSONObject> > userNodes,
                        ArrayList<ArrayList<JSONObject> > playlistNodes, 
                        ArrayList<ArrayList<JSONObject> > songNodes,
                        String filePath) throws FileNotFoundException  {               
        JSONObject dataToWrite = new JSONObject();

        JSONArray userData = new JSONArray();
        for (ArrayList<JSONObject>  users: userNodes) {
            for (JSONObject user: users) {
                userData.add(user);
            }
        }

        // System.out.println(userData);
        dataToWrite.put("users", userData);


        JSONArray plData = new JSONArray();
        for (ArrayList<JSONObject>  pls: playlistNodes) {
            for (JSONObject pl: pls) {
                plData.add(pl);
            }
        }
        // System.out.println(plData);
        dataToWrite.put("playlists", plData);


        JSONArray songData = new JSONArray();
        for (ArrayList<JSONObject>  songs: songNodes) {
            for (JSONObject song: songs) {
                songData.add(song);
            }
        }
        // System.out.println(songData);
        dataToWrite.put("songs", songData);

        PrintWriter pw = new PrintWriter(filePath);
        pw.write(dataToWrite.toJSONString());
          
        pw.flush();
        pw.close();
    }

    private static Queue<Object> getTaskQueue() {
        return new LinkedList<>();
    }

    // Poor man's hash table of fixed size 3 for integers
    private static int getNodeToManageForInt(Object id) {
        String identifier = (String)id;
        int intId = Integer.parseInt(identifier);
        return intId % 3;
    }

    // Poor man's hash table of fixed size 3 for strings
    private static int getNodeToManageForString(Object id) {
        String identifier = (String)id;
        int sum = 0;
        for (int i = 0; i < identifier.length(); i++) {
            sum = sum + identifier.charAt(i);
        }
        return sum % 3;
    }

    private static Object readJSONFile(String filePath) {
        try {
            JSONParser parser = new JSONParser();
            return parser.parse(new FileReader(filePath));
        } catch(FileNotFoundException fe)
        {
            fe.printStackTrace();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return new Object();
    }

    private static ArrayList<ArrayList<JSONObject> > initNodes(String tableName, String key, String type, Object data) {
        int nodeCount = 3;
        ArrayList<ArrayList<JSONObject> > Nodes = 
              new ArrayList<ArrayList<JSONObject> >(nodeCount);
        ArrayList<JSONObject> u1 = new ArrayList<JSONObject>();
        ArrayList<JSONObject> u2 = new ArrayList<JSONObject>();
        ArrayList<JSONObject> u3 = new ArrayList<JSONObject>();
        Nodes.add(u1);
        Nodes.add(u2);
        Nodes.add(u3);

        JSONObject jsonObject = (JSONObject)data;
        JSONArray entries = (JSONArray)jsonObject.get(tableName);
        for(Object entry : entries)
        {
            JSONObject jEntry= (JSONObject)entry;
            int nodeID = (type == "string")  ? getNodeToManageForString(jEntry.get(key)) 
                            : getNodeToManageForInt(jEntry.get(key));
            ArrayList<JSONObject> copy = Nodes.get(nodeID);
            copy.add(jEntry);
            Nodes.set(nodeID, copy);
        }
        return Nodes;
    }        
}

package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.*;
import java.util.stream.*;

/**
 * Sample of stateless operator using Gson
 */
public class SpeedBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

    private static Logger logger = Logger.getLogger("SpeedBoltLogger");
    private OutputCollector collector;
    private static final int taille = 10;
    private static final int pas = 5;
    private int currentNumber = 0;
    private Map<Integer, Deque<TopDTO>> runners;

    public SpeedBolt() {
        this.runners = new HashMap();
    }

    private void addTop(int id, TopDTO value) {
        if (runners.get(id).size() == taille) {
            runners.get(id).removeFirst();
        }
        runners.get(id).addLast(value);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            String n = t.getValueByField("json").toString();
            logger.info("tuple reçu : " + n);

            // Parser le JSON avec Gson
            JsonObject json = JsonParser.parseString(n).getAsJsonObject();

            // Liste des runners
            JsonArray runners = json.getAsJsonArray("runners");
            List<JsonObject> result = new ArrayList<>();
            if(currentNumber < pas) {
                this.currentNumber++;
            }   

            for (int i = 0; i < runners.size(); i++) {
                JsonObject r = runners.get(i).getAsJsonObject();
                int id = r.get("id").getAsInt();
                int top = r.get("top").getAsInt();

                int tour = r.get("tour").getAsInt();
                int cellule = r.get("cellule").getAsInt();
                int maxcel = r.get("maxcel").getAsInt();

                int progression = tour * maxcel + cellule;
                
                this.runners.putIfAbsent(id, new ArrayDeque(taille));
                this.addTop(id, new TopDTO(top, progression));

                float vitesse = (this.runners.get(id).getLast().cellParcourues() - this.runners.get(id).getFirst().cellParcourues()) / this.runners.get(id).size();

                JsonObject ret = new JsonObject();

                String tops = this.runners.get(id).stream()
                            .map(e -> "" + e.top())
                            .collect(Collectors.joining("-"));

                ret.addProperty("id", id);
                ret.addProperty("tops", tops);
                ret.addProperty("vitesse", vitesse);
            
                if(this.currentNumber == pas) {
                    collector.emit(new Values(ret));
                    logger.info("Runner " + id + " => " + ret.toString());
                }
            }
            if(currentNumber >= pas) {
                this.currentNumber = 0;
            } 
            collector.ack(t);

        } catch (Exception e) {
            System.err.println("Empty tuple or invalid JSON: " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {
    }
}
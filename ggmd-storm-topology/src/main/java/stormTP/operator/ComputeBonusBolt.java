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
public class ComputeBonusBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;
    private Map<Integer, List<Integer>> mapTops;
    private Map<Integer, Integer> bonusScores;

    private static Logger logger = Logger.getLogger("ComputeBonusBoltLogger");
    private OutputCollector collector;

    public ComputeBonusBolt() {
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapTops = new HashMap();
        this.bonusScores = new HashMap();
    }

    @Override
    public void execute(Tuple t) {
        try {
            // Récupérer le JSON depuis le tuple
            String n = t.getValueByField("json").toString();
            logger.info("tuple reçu : " + n);

            // Parser le JSON avec Gson
            JsonObject json = JsonParser.parseString(n).getAsJsonObject();

            int id = json.get("id").getAsInt();
            int top = json.get("top").getAsInt();
            String rangStr = json.get("rang").getAsString();
            int rang = Integer.parseInt(rangStr.split(" ")[0]);
            int total = json.get("total").getAsInt();
            int maxcel = json.get("maxcel").getAsInt();

            mapTops.putIfAbsent(id, new ArrayList());
            if(mapTops.get(id).size() >= 15){
                bonusScores.putIfAbsent(id, 0);
                int old = bonusScores.get(id);
                bonusScores.put(id, old + (10-rang));
                mapTops.get(id).clear();
            }

            mapTops.get(id).add(rang);

            JsonObject ret = new JsonObject();

            String tops = mapTops.get(id).stream()
                            .map(e -> "" + e)
                            .collect(Collectors.joining("-"));

            ret.addProperty("id", id);
            ret.addProperty("tops", tops);
            ret.addProperty("score", bonusScores.get(id));

            collector.emit(new Values(ret));
            logger.info("Runner " + id + " => " + ret.toString());
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
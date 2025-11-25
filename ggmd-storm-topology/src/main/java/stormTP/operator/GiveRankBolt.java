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
public class GiveRankBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

    private static Logger logger = Logger.getLogger("GiveRankBoltLogger");
    private OutputCollector collector;

    public GiveRankBolt() {
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple t) {
        try {
            // Récupérer le JSON depuis le tuple
            String n = t.getValueByField("json").toString();

            // Parser le JSON avec Gson
            JsonObject json = JsonParser.parseString(n).getAsJsonObject();

            // Timestamp
            long timestamp = json.get("timestamp").getAsLong();
            System.out.println("Timestamp: " + timestamp);

            // Liste des runners
            JsonArray runners = json.getAsJsonArray("runners");
            JsonObject newJson = new JsonObject();

            List<JsonObject> result = new ArrayList<>();
            // Étape 1 : calculer progression de chaque runner
            List<Integer> progressions = new ArrayList<>();

            for (int i = 0; i < runners.size(); i++) {
                JsonObject r = runners.get(i).getAsJsonObject();

                int tour = r.get("tour").getAsInt();
                int cellule = r.get("cellule").getAsInt();
                int maxcel = r.get("maxcel").getAsInt();

                int progression = tour * maxcel + cellule;
                progressions.add(progression);
            }

            // Étape 2 : calcul du rang pour chacun
            for (int i = 0; i < runners.size(); i++) {
                JsonObject r = runners.get(i).getAsJsonObject();

                int id = r.get("id").getAsInt();
                int top = r.get("top").getAsInt();
                int tour = r.get("tour").getAsInt();
                int cellule = r.get("cellule").getAsInt();
                int total = r.get("total").getAsInt();
                int maxcel = r.get("maxcel").getAsInt();

                int progression = tour * maxcel + cellule;

                // calcul du rang = combien ont une meilleure progression
                int rang = 1;
                String rangStr = "";
                int c = 0;
                for (int p : progressions) {
                    if (p > progression) {
                        rang++;
                    }
                    if (p == progression) {
                        c++;
                    }
                }
                if(c>=2) {
                    rangStr = " ex";
                }


                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("id", id);
                jsonObj.addProperty("top", top);
                jsonObj.addProperty("rang", rang + rangStr);
                jsonObj.addProperty("total", total);
                jsonObj.addProperty("maxcel", maxcel);

                result.add(jsonObj);

                logger.info("Runner " + id + " => " + jsonObj.toString());
                collector.emit(new Values(jsonObj));
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
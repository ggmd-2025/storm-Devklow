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

/**
 * Sample of stateless operator using Gson
 */
public class MyTortoiseBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

    private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
    private OutputCollector collector;

    public MyTortoiseBolt() {
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

            for (int i = 0; i < runners.size(); i++) {
                JsonObject runner = runners.get(i).getAsJsonObject();

                int id = runner.get("id").getAsInt();
                int top = runner.get("top").getAsInt();
                int tour = runner.get("tour").getAsInt();
                int cellule = runner.get("cellule").getAsInt();
                int total = runner.get("total").getAsInt();
                int maxcel = runner.get("maxcel").getAsInt();

                if (id == 3) {
                    logger.info("=> " + id + " found!");

                    newJson.addProperty("id", id);
                    newJson.addProperty("top", top);
                    newJson.addProperty("nom", "Runner3"); // ici tu peux mettre un vrai nom si disponible
                    newJson.addProperty("nbCellsParcourus", tour * maxcel + cellule); // exemple de calcul
                    newJson.addProperty("total", total);
                    newJson.addProperty("maxcel", maxcel);

					logger.info("My Tortoise " + newJson.toString());
                }
            }

            // logger.info("=> " + n + " treated!");
            collector.emit(t, new Values(newJson.toString())); // réémettre le JSON
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
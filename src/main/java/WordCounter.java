import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Tuple2;


public class WordCounter {
    private String word;
    private Integer count;

    public WordCounter(Tuple2<String, Integer> counter) {
        this.word = counter._1;
        this.count = counter._2;
    }

    public String getWord() {
        return word;
    }

    public Integer getCount() {
        return count;
    }
}

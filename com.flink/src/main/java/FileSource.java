import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FileSource implements SourceFunction<String> {

    private String filePath;

    public FileSource(String filePath) {
        this.filePath = filePath;
    }

    Random ran = new Random();

    private BufferedReader reader;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //读取数据
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));

        String line = null;
        while ((line = reader.readLine()) != null) {
            TimeUnit.MILLISECONDS.sleep(ran.nextInt(500));
            sourceContext.collect(line);
        }
    }

    @Override
    public void cancel() {
        if (null != reader) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

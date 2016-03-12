package net.unir.bdva.ejemploFlumeHive.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by mikel on 12/3/16.
 */
public class ElasticSearchInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchInterceptor.class);


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        logger.info("Receiving event...");

        byte[] body = event.getBody();
        String bodyStr = new String(body);

        Map<String, String> headers = event.getHeaders();

        String[] blankSplitted = bodyStr.split(" ");


        String fechaHora = String.format("%s %s", blankSplitted[0], blankSplitted[1]);
        headers.put("fechahora", fechaHora);

        String ipRemota = blankSplitted[2].replace("\"", "");
        headers.put("ip_remota", ipRemota);

        String idUsuario = blankSplitted[3].replace("\"", "");
        headers.put("id_usuario", idUsuario);

        String trueClientIP = blankSplitted[7];
        headers.put("true_client_ip", trueClientIP);

        event.setHeaders(headers);

        logger.info("Sending evento to elasticsearch...");

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Iterator<Event> iterator = list.iterator(); iterator.hasNext();)
        {
            Event next = intercept(iterator.next());
            if (next == null)
            {
                iterator.remove();
            }
        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ElasticSearchInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

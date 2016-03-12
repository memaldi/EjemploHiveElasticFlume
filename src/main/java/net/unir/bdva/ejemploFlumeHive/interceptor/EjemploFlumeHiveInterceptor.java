package net.unir.bdva.ejemploFlumeHive.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Created by mikel on 9/3/16.
 */
public class EjemploFlumeHiveInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(EjemploFlumeHiveInterceptor.class);

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        
        // Cogemos el contenido del evento
        byte[] bytes = event.getBody();
        String body = new String(bytes);
        // Vamos desglosando y metiendolo en un JSON
        JSONObject jsonObject = new JSONObject();

        String[] blankSplitted = body.split(" ");

        String fechaHora = String.format("%s %s", blankSplitted[0], blankSplitted[1]);
        jsonObject.put("fechahora", fechaHora);

        String ipRemota = blankSplitted[2].replace("\"", "");
        jsonObject.put("ip_remota", ipRemota);

        String idUsuario = blankSplitted[3].replace("\"", "");
        jsonObject.put("id_usuario", idUsuario);

        String trueClientIP = blankSplitted[7];
        jsonObject.put("true_client_ip", trueClientIP);

        logger.info(jsonObject.toString());

        // Metemos el JSON en el evento
        event.setBody(jsonObject.toString().getBytes());

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
            return new EjemploFlumeHiveInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

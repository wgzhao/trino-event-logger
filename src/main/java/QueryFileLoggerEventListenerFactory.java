import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 *
 * @author zhaowg
 */
public class QueryFileLoggerEventListenerFactory implements
	EventListenerFactory {
	public String getName() {
		return "query-event-logger";
	}

	@Override
	public EventListener create(Map<String, String> config) {
		return new QueryFileLoggerEventListener(config);
	}
}

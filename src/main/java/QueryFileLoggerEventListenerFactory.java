import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 *
 * @author zhaowg
 * @date 2020-11-13
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

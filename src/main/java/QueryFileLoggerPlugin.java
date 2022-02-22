import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;

/**
 *
 * @author zhaowg
 */
public class QueryFileLoggerPlugin
        implements Plugin
{

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        EventListenerFactory listenerFactory = new QueryFileLoggerEventListenerFactory();
        return ImmutableList.of(listenerFactory);
    }
}

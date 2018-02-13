package info.bitrich.xchangestream.binance;

import com.fasterxml.jackson.databind.JsonNode;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BinanceStreamingService {
    private static final Logger LOG = LoggerFactory.getLogger(BinanceStreamingService.class);

    private Map<CurrencyPair, BinanceProductStreamingService> productStreamingServices;
    private Map<CurrencyPair, Observable<JsonNode>> productSubscriptions;
    private final String baseUri;

    public BinanceStreamingService(String _baseUri) {
        baseUri = _baseUri;
        productStreamingServices = new HashMap<>();
        productSubscriptions = new HashMap<>();
    }

    public Observable<JsonNode> subscribeChannel(
            CurrencyPair currencyPair,
            Object... args) {
        if (!productStreamingServices.containsKey(currencyPair)) {
            String symbolUri = baseUri + currencyPair.base.toString().toLowerCase() + currencyPair.counter.toString().toLowerCase() + "@depth";
            BinanceProductStreamingService productStreamingService = new BinanceProductStreamingService(symbolUri,
                    currencyPair);
            productStreamingService.connect().blockingAwait();
            Observable<JsonNode> productSubscription = productStreamingService
                    .subscribeChannel(currencyPair.toString(), args);
            productStreamingServices.put(currencyPair, productStreamingService);
            productSubscriptions.put(currencyPair, productSubscription);
        }

        return productSubscriptions.get(currencyPair);
    }

    public Completable disconnect() {
        Completable last = null;
        List<BinanceProductStreamingService> streams = new ArrayList<>(productStreamingServices.values());

        for (int i = 0; i < streams.size(); i++) {
            BinanceProductStreamingService stream = streams.get(i);
            final int idx = i + 1;
            stream.disconnect().subscribe(() -> System.out.println("Binance disconnected [" + idx + "/" + streams.size() + "]"));
        }
        return last;
    }

    public boolean isAlive() {
        for (BinanceProductStreamingService service : productStreamingServices.values()) {
            if (service.isSocketOpen()) {
                return false;
            }
        }
        return true;
    }
}

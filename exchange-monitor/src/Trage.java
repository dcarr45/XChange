import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.TradeService;


import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Trage {
    private StreamingExchange highEx;
    private StreamingExchange lowEx;

    private String highName;
    private String lowName;

    private double highPrice;
    private double lowPrice;

    private CurrencyPair currencyPair;

    private Observable<Double> spreadObservable;
    private Disposable spreadSubscription;

    private boolean updated;

    private Map<String, BidAsk> bidAskMap;
    private Map<String, Disposable> subscriptionMap;
    private Map<String, Observable<BidAsk>> streamMap;
    private Map<String, Queue<Double>> lastTenMap = new ConcurrentHashMap<>();

    private Map<String, StreamingExchange> connectedExchanges;

    private Thread pollerThread;
    private Thread keepAliveThread;

    private static final int KEEPALIVE_QUEUE_SIZE = 20;
    private static final long KEEPALIVE_INTERVAL = 1000L;

    private double spread;
    private double spreadPercentage;

    public class BidAsk {
        double bid;
        double ask;

        public BidAsk() {
            this.bid = 0;
            this.ask = Double.MAX_VALUE;
        }

        BidAsk(double bid, double ask) {
            this.bid = bid;
            this.ask = ask;
        }

        public void setBid(double bid) {
            this.bid = bid;
        }

        public void setAsk(double ask) {
            this.ask = ask;
        }

        @Override
        public String toString() {
            return "High Bid: " + bid + ", Low Ask: " + ask;
        }
    }

    public Trage(StreamingExchange left, StreamingExchange right, CurrencyPair pair) throws IllegalArgumentException, IllegalStateException {
        this.bidAskMap = new ConcurrentHashMap<>();
        this.streamMap = new ConcurrentHashMap<>();
        this.subscriptionMap = new ConcurrentHashMap<>();

        this.connectedExchanges = new ConcurrentHashMap<>();

        this.currencyPair = pair;

        System.out.println("INITIALIZE");

        if (!this.connectExchange(left) || !this.initializeStream(left)) {
            highEx = left;
            destroy();
            throw new IllegalStateException("Failed to connect to " + left.getName());
        }
        if (!this.connectExchange(right) || !this.initializeStream(right)){
            highEx = left;
            lowEx = right;
            destroy();
            throw new IllegalStateException("Failed to connect to " + right.getName());
        }

        this.initializeSpreadWatch(left, right);
    }

    //TODO:: Execute trade
    //TODO:: Destroy

    public boolean connectExchange(StreamingExchange ex) {
        CurrencyPair pair = this.currencyPair;
        if (ex.getName().contains("GDAX") || ex.getName().contains("Bitfinex")) {
            if (pair.counter.toString().equals("USDT")) {
                return false;
            }
        }

        ProductSubscription prodSub = ProductSubscription.create().addOrderbook(pair).build();

        if (ex.connect(prodSub).blockingAwait(30, TimeUnit.SECONDS)) {
            System.out.println("Connected to " + ex.getName());
        } else {
            connectedExchanges.remove(ex.getName());
            return false;
        }

        connectedExchanges.put(ex.getName(), ex);

        try {
            Thread.sleep(new Long(3000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return true;
    }

    public boolean initializeStream(StreamingExchange ex) {
        String exName = ex.getName();
        bidAskMap.put(exName, new BidAsk());

        CurrencyPair pair = this.currencyPair;
        if (exName.contains("GDAX") || exName.contains("Bitfinex")) {
            if (pair.counter.toString().equals("USDT")) {
                pair = new CurrencyPair(pair.base, Currency.USD);
            }
        }

        try {
            Observable<BidAsk> observable = ex.getStreamingMarketDataService().getOrderBook(pair).map(orderBook -> {
                BidAsk bidAsk = bidAskMap.get(exName);

                List<LimitOrder> bids = orderBook.getBids();
                List<LimitOrder> asks = orderBook.getAsks();

                Double bid = null;
                Double ask = null;

                if (bids.size() > 0) {
                    bid = bids.get(0).getLimitPrice().doubleValue();
                }
                if (asks.size() > 0) {
                    ask = asks.get(0).getLimitPrice().doubleValue();
                }

                if (bid != null) {
                    boolean higher = bid > bidAsk.bid;

                    bidAsk.setBid(bid);
                    if (higher) {
                        System.out.println(Thread.currentThread().getId()
                                + ": Found higher bid, " + exName
                                + ": " + bidAsk.toString());

                        this.updated = true;
                    }
                }

                if (ask != null) {
                    boolean lower = ask < bidAsk.ask;

                    bidAsk.setAsk(ask);
                    if (lower) {
                        System.out.println(Thread.currentThread().getId()
                                + ": Found lower ask, " + exName
                                + ": " + bidAsk.toString());
                        this.updated = true;
                    }
                }

                return bidAsk;
            }).share();

            Disposable sub = observable.subscribe(bidAsk -> {
                Queue<Double> lastTen = lastTenMap.get(exName);

                if (lastTen != null) {
                    while (lastTen.size() >= KEEPALIVE_QUEUE_SIZE) {
                        lastTen.remove();
                    }
                    lastTen.add(bidAsk.bid * bidAsk.ask);
                }
            });

            subscriptionMap.put(exName, sub);
            streamMap.put(exName, observable);


            Double[] lastTenInit = new Double[KEEPALIVE_QUEUE_SIZE];
            Arrays.fill(lastTenInit, 0.0);
            lastTenMap.put(exName, new LinkedList<>(Arrays.asList(lastTenInit)));

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public void initializeSpreadWatch(StreamingExchange leftEx, StreamingExchange rightEx) {
        //guess and check
        this.highEx = leftEx;
        this.highName = leftEx.getName();

        this.lowEx = rightEx;
        this.lowName = rightEx.getName();

        this.spreadObservable = Observable.combineLatest(streamMap.get(leftEx.getName()), streamMap.get(rightEx.getName()),
                (BidAsk left, BidAsk right) -> {
                    bidAskMap.put(leftEx.getName(), left);
                    bidAskMap.put(rightEx.getName(), right);

                    double leftPrice = left.bid;
                    double rightPrice = right.ask;

                    this.highPrice = leftPrice;
                    this.lowPrice = rightPrice;

                    if (highPrice < lowPrice) {
                        highEx = rightEx;
                        highPrice = right.bid;
                        highName = rightEx.getName();

                        lowEx = leftEx;
                        lowPrice = left.ask;
                        lowName = leftEx.getName();
                    } else {
                        highEx = leftEx;
                        highName = leftEx.getName();

                        lowEx = rightEx;
                        lowName = lowEx.getName();
                    }

                    if (this.updated) {
                        spread = highPrice - lowPrice;
                        spreadPercentage = spread * 100.0 / highPrice;

//                        System.out.println(this.toString());

                        this.updated = false;
                    }

                    return spreadPercentage;
                }
        ).share();
    }

    private static double fetchPrice(Exchange exchange, CurrencyPair pair) throws ExchangeException {
        MarketDataService mds = exchange.getMarketDataService();

        if (exchange.getName().contains("GDAX") || exchange.getName().contains("Bitfinex")) {
            if (pair.counter.toString().equals("USDT")) {
                pair = new CurrencyPair(pair.base, Currency.USD);
            }
        }

        try {
            Trades trades = new Trades(mds.getTrades(pair).getTrades(), Trades.TradeSortType.SortByTimestamp);

            double price = trades.getTrades().get(0).getPrice().doubleValue();

            System.out.println(exchange.getName());
            System.out.println("\t" + trades.getTrades().subList(0, 5));

            return price;
        } catch (IOException | ExchangeException e) {
            e.printStackTrace();
            if (e instanceof ExchangeException) {
                throw new ExchangeException(exchange.getName());
            }
            System.out.println("No " + pair.toString() + " ticker found for " + exchange.getClass().getName());

            return 0;
        }
    }


    public void execute() {
        TradeService highTS = this.highEx.getTradeService();
        TradeService lowTS = this.lowEx.getTradeService();

        System.out.println("Executing on" + this.toString());

        if (currencyPair.base.toString().equals("ETH")
                && currencyPair.counter.toString().equals("BTC")
                && (highEx.getName().contains("Poloniex") || lowEx.getName().contains("Poloniex"))) {

            BigDecimal limitPrice = new BigDecimal(highPrice * .99999);

            LimitOrder order = new LimitOrder.Builder(Order.OrderType.ASK, currencyPair)
                    .originalAmount(new BigDecimal(0.0001)).limitPrice(limitPrice).build();

//            StreamingExchange highStream = StreamingExchangeFactory.INSTANCE.createExchange(PoloniexStreamingExchange.class.getName());;
//
//            Disposable d = observable.takeWhile(trade -> {
//                System.out.println("Inside takeWhile, returning false");
//                return false;
//            }).subscribe(trade -> {
//                System.out.println("Inside subscribe, " + Thread.currentThread().getId() + ": " + trade.getPrice().doubleValue());
//            }, throwable -> {
//                throwable.printStackTrace();
//            }, () -> {
//                System.out.println("Completed!");
//            });
//
//            observable.subscribe(trade -> System.out.println("Other trades..." + trade.toString()));
//
//            System.out.println("This thread is: " + Thread.currentThread().getId());
//
//            Thread.sleep(new Long(30000));
//
//            if (!d.isDisposed()) {
//                System.out.println("Manually disposed");
//                d.dispose();
//            } else {
//                System.out.println("It does automatically dispose!!!");
//            }
//
//            highStream.disconnect().subscribe(() -> System.out.println("Disconnected from " + highStream.getName()));
        }

    }

    public void destroy() {
        for (StreamingExchange ex : connectedExchanges.values()) {
            ex.disconnect().subscribe(() -> System.out.println("Disconnected from " + ex.getName()));
        }
        dispose();
        if (pollerThread != null) {
            pollerThread.interrupt();
        }
        if (keepAliveThread != null) {
            keepAliveThread.interrupt();
        }
    }

    public void dispose() {
        //stop writing to lastTenMap
        for (Disposable d : subscriptionMap.values()) {
            d.dispose();
        }
        if (spreadSubscription != null) {
            spreadSubscription.dispose();
        }
    }

    public void keepAlive() {
        //poller
        pollerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    for (Queue<Double> lastTen : lastTenMap.values()) {
                        //remove one
                        lastTen.poll();
                    }
                    try {
                        Thread.sleep(KEEPALIVE_INTERVAL);
                    } catch (Exception e) {
                        System.out.println("Poller interrupted - exiting");
                    }
                }
            }
        });

        keepAliveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Long additionalWait;

                    while (true) {
                        additionalWait = 0L;
                        boolean reinit = false;
                        boolean noReinit = false;

                        final StreamingExchange high = highEx;
                        final StreamingExchange low = lowEx;

                        final String hName = high.getName();
                        final String lName = low.getName();

                        //reinitialize exchanges - connect will timeout after 30 seconds
                        //but clearing lastTen means it'll just try again next time

                        if (checkUp(high)) {
                            highEx = StreamingExchangeFactory.INSTANCE.createExchange(high.getClass().getName());
                            lastTenMap.remove(hName);

                            try {
                                if (connectExchange(highEx)) {
                                    highEx.remoteInit();
                                    initializeStream(highEx);

//                                    additionalWait += 5000L;
                                    reinit = true;
                                } else {
                                    noReinit = true;
                                }
                            } catch (Exception e) {
                                if (connectedExchanges.containsKey(hName)) {
                                    highEx.disconnect();
                                    connectedExchanges.remove(hName);
                                }
                                if (subscriptionMap.containsKey(hName)) {
                                    subscriptionMap.get(hName).dispose();
                                }
                                additionalWait += 5000L;
                                noReinit = true;
                            }
                        }

                        if (checkUp(low)) {
                            lowEx = StreamingExchangeFactory.INSTANCE.createExchange(low.getClass().getName());
                            lastTenMap.remove(lName);

                            try {
                                if (connectExchange(lowEx)) {
                                    initializeStream(lowEx);

//                                    additionalWait += 5000L;
                                    reinit = true;
                                } else {
                                    noReinit = true;
                                }
                            } catch (Exception e) {
                                if (connectedExchanges.containsKey(lName)) {
                                    lowEx.disconnect();
                                    connectedExchanges.remove(lName);
                                }
                                if (subscriptionMap.containsKey(lName)) {
                                    subscriptionMap.get(lName).dispose();
                                }
                                additionalWait += 5000L;
                                noReinit = true;
                            }
                        }

                        if (reinit && !noReinit) {
                            boolean initialized = false;
                            try {
                                initializeSpreadWatch(highEx, lowEx);
                                initialized = true;
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("Failed to initialize spread observable");

                            }

                            if (initialized) {
                                //don't continue keeping alive until we actually have live streams
                                try {
                                    spreadObservable.timeout(30, TimeUnit.SECONDS).blockingFirst();
                                    System.out.println("Received first new spread for: " + Trage.this.toString());
                                    spreadSubscription = spreadObservable.subscribe();
                                } catch (Exception e) {
                                    System.out.println("Failed to initialize spread...(" + lName + ", " + hName + ")");
                                    additionalWait = 0L;
                                }
                            }
                        }

                        Thread.sleep(KEEPALIVE_INTERVAL + additionalWait);
                    }
                } catch (Exception e) {
                    if (!(e instanceof InterruptedException)) {
                        e.printStackTrace();
                    }
                } finally {
                    System.out.println(Trage.this.toString() + "\nKeepAlive interrupted! Exiting...");
                }
            }
        });

        pollerThread.start();
        keepAliveThread.start();
    }

    private boolean checkUp(StreamingExchange ex) {
        String exName = ex.getName();
        double waitTime = KEEPALIVE_INTERVAL * KEEPALIVE_QUEUE_SIZE / 1000;

        Queue<Double> lastTen = lastTenMap.get(exName);
        Disposable subscription = subscriptionMap.get(exName);

        if (lastTen == null || lastTen.isEmpty()) {
            System.out.println(waitTime + " seconds of silence on " + exName + " " + currencyPair.toString()
                    + " - reinitializing from keepAlive");

            try {
                if (!ex.isAlive()) {
                    System.out.println(exName + " was dead...");
                }
            } catch (Exception e) {
                System.out.println("Ugh");
            }

            Completable disconnect = ex.disconnect();
            if (disconnect != null) {
                if (disconnect.blockingAwait(60, TimeUnit.SECONDS)) {
                    System.out.println("Disconnected from " + exName);
                } else {
                    System.out.println("Failed to disconnect from " + exName);
                }
            } else {
                System.out.println("No disconnect callback, hoping for the best");
            }

            connectedExchanges.remove(exName);
            subscription.dispose();
            if (spreadSubscription != null) {
                spreadSubscription.dispose();
            }

            return true;
        } else if (subscription == null || subscription.isDisposed()) {
            System.out.println(exName + " " + currencyPair.toString() + " is disposed! Reinitializing");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
//        sb.append(highName + " - " + lowName);
//        sb.append("\n\t" + highName + ": " + highPrice.toString());
//        sb.append("\n\t" + lowName + ": " + lowPrice.toString());
        sb.append("\t" + this.highEx.getName() + " > " + this.lowEx.getName());
        sb.append("\n\tCurrency Pair: " + this.currencyPair);
        sb.append("\n\t\tSpread: " + spread + " " + this.currencyPair.counter);
        sb.append("\n\t\tSpread Percentage: " + String.format("%.5g", spreadPercentage) + "%");
        return sb.toString();
    }

    public String printEval() {
        final double hPrice = highPrice;
        final double lPrice = lowPrice;

        boolean isDollars = currencyPair.counter.equals(Currency.USDT);
        double holding = isDollars ? 10000
                : currencyPair.counter.equals(Currency.BTC) ? 1 : 10;

        double highQuant = holding / hPrice;

        double highPost = highQuant * hPrice * (1 - Barb.exchangeFees.get(highName));
        double lowPost = holding * (1 - Barb.exchangeFees.get(lowName)) / lPrice;

        double lowHold = highPost - highEx.getWithdrawalFees().get(currencyPair.counter);
        double highHold = lowPost - lowEx.getWithdrawalFees().get(currencyPair.base);

        StringBuilder sb = new StringBuilder();
        sb.append("Result after " + holding + " " + currencyPair.counter + ", " + highQuant + " " + currencyPair.base);
        sb.append(":\n");
        sb.append("\t" + highHold + " " + currencyPair.base);
        if (isDollars) {
            sb.append(" = " + (highHold * hPrice) + " " + currencyPair.counter);
        }
        sb.append(" on the high exchange\n");
        sb.append("\t" + lowHold + " " + currencyPair.counter + " on the low exchange\n");
        sb.append("Portfolio value: " + (highHold * hPrice + lowHold) + " " + currencyPair.counter + "\n");
        return sb.toString();
    }

    public StreamingExchange getHighEx() {
        return highEx;
    }

    public StreamingExchange getLowEx() {
        return lowEx;
    }

    public Double getSpread() {
        return spread;
    }

    public String getHighName() {
        synchronized (highName) {
            return highName;
        }
    }

    public String getLowName() {
        synchronized (lowName) {
            return lowName;
        }
    }

    public Double getHighPrice() {
        return highPrice;
    }

    public Double getLowPrice() {
        return lowPrice;
    }

    public CurrencyPair getCurrencyPair() {
        return currencyPair;
    }

    public Map<String, BidAsk> getBidAskMap() {
        return bidAskMap;
    }

    public Double getSpreadPercentage() {
        return spreadPercentage;
    }

    public Observable<Double> getSpreadObservable() {
        return spreadObservable;
    }

    public Thread getKeepAliveThread() {
        return keepAliveThread;
    }

    public boolean isAlive(String exName) {
        Queue<Double> lastTen = this.lastTenMap.get(exName);
        return lastTen != null && lastTen.size() > 0;
    }

    public void setSpreadSubscription(Disposable d) {
        this.spreadSubscription = d;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || ! (obj instanceof Trage)) {
            return false;
        }
        Trage t = (Trage) obj;
        return ((t.getHighName().equals(this.getHighName()) && t.getLowName().equals(this.getLowName())) ||
                (t.getLowName().equals(this.getHighName()) && t.getHighName().equals(this.getLowName())))
                && t.getCurrencyPair().equals(this.currencyPair);
    }

    @Override
    public int hashCode() {
        return Objects.hash(highName, lowName, currencyPair);
    }
}

package org.knowm.xchange.bitfinex.v1;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knowm.xchange.BaseExchange;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitfinex.v1.service.BitfinexAccountService;
import org.knowm.xchange.bitfinex.v1.service.BitfinexMarketDataService;
import org.knowm.xchange.bitfinex.v1.service.BitfinexMarketDataServiceRaw;
import org.knowm.xchange.bitfinex.v1.service.BitfinexTradeService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.utils.nonce.AtomicLongIncrementalTime2013NonceFactory;

import si.mazi.rescu.SynchronizedValueFactory;

public class BitfinexExchange extends BaseExchange implements Exchange {

  private SynchronizedValueFactory<Long> nonceFactory = new AtomicLongIncrementalTime2013NonceFactory();

  @Override
  protected void initServices() {
    this.marketDataService = new BitfinexMarketDataService(this);
    this.accountService = new BitfinexAccountService(this);
    this.tradeService = new BitfinexTradeService(this);
  }

  @Override
  public ExchangeSpecification getDefaultExchangeSpecification() {

    ExchangeSpecification exchangeSpecification = new ExchangeSpecification(this.getClass().getCanonicalName());
    exchangeSpecification.setSslUri("https://api.bitfinex.com/");
    exchangeSpecification.setHost("api.bitfinex.com");
    exchangeSpecification.setPort(80);
    exchangeSpecification.setExchangeName("BitFinex");
    exchangeSpecification.setExchangeDescription("BitFinex is a bitcoin exchange.");

    return exchangeSpecification;
  }

  @Override
  public SynchronizedValueFactory<Long> getNonceFactory() {

    return nonceFactory;
  }

  @Override
  public void remoteInit() throws IOException, ExchangeException {

    BitfinexMarketDataServiceRaw dataService = (BitfinexMarketDataServiceRaw) this.marketDataService;
    List<CurrencyPair> currencyPairs = dataService.getExchangeSymbols();
    exchangeMetaData = BitfinexAdapters.adaptMetaData(currencyPairs, exchangeMetaData);

  }

  @Override
  public Map<Currency, Double> getWithdrawalFees() {
    Map<Currency, Double> wdFees = new HashMap<>();

    wdFees.put(Currency.BTC, 0.008);
    wdFees.put(Currency.ETH, 0.01);
    wdFees.put(Currency.USDT, 20.0);
    wdFees.put(Currency.USD, 20.0);

    return wdFees;
  }

}

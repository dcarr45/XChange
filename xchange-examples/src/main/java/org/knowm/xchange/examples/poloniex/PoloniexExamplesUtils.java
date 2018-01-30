package org.knowm.xchange.examples.poloniex;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.poloniex.PoloniexExchange;

/**
 * @author Zach Holmes
 */

public class PoloniexExamplesUtils {

  public static Exchange getExchange() {

    ExchangeSpecification spec = new ExchangeSpecification(PoloniexExchange.class);
    spec.setApiKey("5M3TTPZY-CS1IRQF5-M4UUJPMP-XJDQIBJD");
    spec.setSecretKey("638df1aa261742ef3bf1ee44a0f6c8b8edcf85217b9f67ee5ec07946406919d7bc60962db4b271f3e16fc9ef0c8c679d19a48433df5a3220f4748c1d76a041eb");

    return ExchangeFactory.INSTANCE.createExchange(spec);
  }
}

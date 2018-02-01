import info.bitrich.xchangestream.binance.BinanceStreamingExchange;
import info.bitrich.xchangestream.bitfinex.BitfinexStreamingExchange;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;

import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.gdax.GDAXStreamingExchange;
import info.bitrich.xchangestream.poloniex2.PoloniexStreamingExchange;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.apache.commons.io.FileUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.bitfinex.v1.BitfinexExchange;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.gdax.GDAXExchange;
import org.knowm.xchange.poloniex.Poloniex;
import org.knowm.xchange.poloniex.PoloniexExchange;
import org.knowm.xchange.utils.CertHelper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Barb {

    public static final Set<String> EXCHANGES = new HashSet<>(Arrays.asList(
            BinanceExchange.class.getName(),
            BitfinexExchange.class.getName(),
            GDAXExchange.class.getName(),
            PoloniexExchange.class.getName()
    ));

    public static final Set<CurrencyPair> PAIRS = new HashSet<>(Arrays.asList(
            CurrencyPair.ETH_USDT,
            CurrencyPair.BTC_USDT,
            CurrencyPair.ETH_BTC
    ));

    public static Map<String, String> streamClasses = new HashMap<>();

    public static final Map<String, Double> exchangeFees = new HashMap<>();

    public static List<ExPairToExcel> excelList = new ArrayList<>();

    public static Exchange initializeExchange(String exName) throws IOException {
        Exchange ex;
        
        ex = ExchangeFactory.INSTANCE.createExchange(exName);

        return ex;
    }

    public static void main(String[] args) throws Exception {
        File excelDir = new File("excel/");

        FileUtils.cleanDirectory(excelDir);

//        if (new ArrayList<String>(Arrays.asList(excelDir.list())).size() > 0) {
//            System.out.println("How bout we empty the excel directory before you lose all the work there chief");
//            System.out.println(Arrays.asList(excelDir.list()));
//            return;
//        }


        List<Exchange> exchanges = new ArrayList<>();

        CertHelper.trustAllCerts();

        for (String exName : EXCHANGES) {
            Exchange ex = initializeExchange(exName);
            ex.getExchangeSpecification().setExchangeSpecificParametersItem("recvWindow", new Long(6000000));

            exchanges.add(ex);
        }


        List<Trage> trages = new ArrayList<>();

        //initialize trages and workbooks for all valid exchange pairs
        for (int i = 0; i < exchanges.size() - 1; i++) {
            Exchange left = exchanges.get(i);

            for (int j = i + 1; j < exchanges.size(); j++) {
                Exchange right = exchanges.get(j);

                Set<CurrencyPair> leftPairs = left.getExchangeMetaData().getCurrencyPairs().keySet();
                Set<CurrencyPair> rightPairs = right.getExchangeMetaData().getCurrencyPairs().keySet();

                ExPairToExcel toExcel = new ExPairToExcel(left.getName(), right.getName());

                for (CurrencyPair pair : PAIRS) {
                    Set<CurrencyPair> adjPairs = new HashSet<>();
                    adjPairs.add(pair);

                    if (pair.counter.toString().equals("USDT")) {
                        adjPairs.add(new CurrencyPair(pair.base, Currency.USD));
                    }

                    if (!Collections.disjoint(leftPairs, adjPairs) && !Collections.disjoint(rightPairs, adjPairs)) {
                        try {
                            Trage trage = new Trage(
                                    StreamingExchangeFactory.INSTANCE.createExchange(streamClasses.get(left.getName())),
                                    StreamingExchangeFactory.INSTANCE.createExchange(streamClasses.get(right.getName())),
                                    pair);

                            trages.add(trage);
                            toExcel.createSheet(pair);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    //avoid exceeding rate limit
                    Thread.sleep(new Long(20));
                }

                excelList.add(toExcel);
            }
        }

        System.out.println("Stuff's been initialized");

        //wait for all first values
        trages.parallelStream().forEach(trage -> {
            Observable<Double> spreadWatch = trage.getSpreadObservable();

            double firstSpread = spreadWatch.blockingFirst();
            System.out.println("First Spread: " + firstSpread);
            System.out.println("Received first spread for: " + trage.toString());

            trage.setSpreadSubscription(spreadWatch.subscribe());
            trage.keepAlive();
        });

        int keepAliveCounter = 0;

        int rowNum = 2;
        try {
            int ITERS = 10000;
            while (rowNum <= ITERS) {
                Collections.sort(trages, Comparator.comparing(Trage::getSpreadPercentage));

                for (Trage trage : trages) {
                    if (!trage.getKeepAliveThread().isAlive()) {
                        keepAliveCounter++;
                        trage.keepAlive();
                    }

                    String highName = trage.getHighName();
                    String lowName = trage.getLowName();

                    ExPairToExcel toExcel = getExcel(highName, lowName);
                    if (toExcel != null) {
                        toExcel.createRow(trage, rowNum);
                    } else {
                        System.out.println("Excel for " + trage.toString() + " was null????");
                    }

                    System.out.println(trage.toString());
                }

                System.out.println();

                Trage highTrage = trages.get(trages.size() - 1);
                System.out.println(highTrage.printEval());

                System.out.println("---------------" + rowNum + "----------------\n");

                try {
                    Thread.sleep(new Long(5000));
                } catch (InterruptedException e) {
                    System.out.println("Thread interrupted\n");
                    break;
                }

                rowNum++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Keep alive restarts: " + keepAliveCounter);
            for (ExPairToExcel toExcel : excelList) {
                toExcel.close();
            }

            for (Trage trage : trages) {
                trage.destroy();
            }
        }

        System.exit(0);
    }


    public static ExPairToExcel getExcel(String leftName, String rightName) {
        for (ExPairToExcel excel : excelList) {
            if ((excel.leftName.equals(leftName) && excel.rightName.equals(rightName))
                    || (excel.leftName.equals(rightName) && excel.rightName.equals(leftName))) {
                return excel;
            }
        }
        return null;
    }

    public static class ExPairToExcel {
        String leftName;
        String rightName;

        Workbook wb;
        FileOutputStream fileOut;

        String fileName;

        Map<CurrencyPair, Sheet> pairToSheet;

        ExPairToExcel(String leftName, String rightName) {
            this.leftName = leftName;
            this.rightName = rightName;

            this.fileName = "excel/" + leftName + rightName + "12.xls";

            wb = new HSSFWorkbook();
            try {
                this.fileOut = new FileOutputStream(fileName);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.out.println("Failed to create output stream for " + fileName);
            }

            pairToSheet = new HashMap<>();
        }

        void createSheet(CurrencyPair pair) {
            Sheet spreadSheet = wb.createSheet(String.join("-", pair.toString().split("/")));
            /*
            L
            O
            L
            spreadsheet
             */
            Row header = spreadSheet.createRow(0);
            header.createCell(1).setCellValue(leftName);
            header.createCell(3).setCellValue(rightName);
            header.createCell(5).setCellValue("Spread");
            header.createCell(6).setCellValue("Spread Percentage");
            header.createCell(7).setCellValue(leftName + " isAlive");
            header.createCell(8).setCellValue(rightName + " isAlive");


            Row row = spreadSheet.createRow(1);
            row.createCell(1).setCellValue("Bid");
            row.createCell(2).setCellValue("Ask");
            row.createCell(3).setCellValue("Bid");
            row.createCell(4).setCellValue("Ask");

            pairToSheet.put(pair, spreadSheet);
        }

        void createRow(Trage trage, int rowNum) {
            Sheet trageSheet = pairToSheet.get(trage.getCurrencyPair());

            Row row = trageSheet.createRow(rowNum);
            Trage.BidAsk left = trage.getBidAskMap().get(leftName);
            Trage.BidAsk right = trage.getBidAskMap().get(rightName);
            row.createCell(0).setCellValue(new Date().getTime() / 1000);
            row.createCell(1).setCellValue(left.bid);
            row.createCell(2).setCellValue(left.ask);
            row.createCell(3).setCellValue(right.bid);
            row.createCell(4).setCellValue(right.ask);

            row.createCell(5).setCellValue(trage.getSpread());
            row.createCell(6).setCellValue(trage.getSpreadPercentage());

            row.createCell(7).setCellValue(trage.isAlive(leftName));
            row.createCell(8).setCellValue(trage.isAlive(rightName));
        }

        void close() {
            try {
                wb.write(fileOut);
                fileOut.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to write to " + fileName);
            }
        }
    }

}


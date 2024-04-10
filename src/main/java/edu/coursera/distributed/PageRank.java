package edu.coursera.distributed;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public final class PageRank {

    private PageRank() { }

    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

        JavaPairRDD<Integer, Double> newRanks =
                sites.join(ranks)
                        .flatMapToPair(siteRank -> {
                            Website website = siteRank._2()._1();
                            Double currentRank = siteRank._2()._2();

                            List<Tuple2<Integer, Double>> contributions = new LinkedList<>();
                            website.edgeIterator().forEachRemaining(
                                    target -> contributions.add(
                                            new Tuple2<>(target, currentRank / (double) website.getNEdges()))
                            );
                            return contributions;
                        });

        return newRanks
                .reduceByKey(Double::sum)
                .mapValues(v -> 0.15 + 0.85 * v);
    }
}

package io.raftkv.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

/**
 * Renders the cluster benchmark as static SVG line charts for the README: one series per cluster size, x = number
 * of concurrent clients (log2 spaced), one y-axis per chart. Colors are the two first categorical slots of a
 * validated palette (checked for colour-vision deficiency separation and surface contrast in light and dark
 * mode); every series has a legend entry and a direct label, so identity never depends on colour alone. The
 * results table in the README is the table view.
 */
final class Charts {

    private static final int WIDTH = 720;
    private static final int HEIGHT = 400;
    private static final int LEFT = 72;
    private static final int RIGHT = 96;
    private static final int TOP = 104;
    private static final int BOTTOM = 56;

    /** Categorical slot 1 (blue) and 2 (orange); dark-mode steps are defined in the style block. */
    private static final String[] SERIES_CLASS = {"s1", "s2", "s3"};

    private Charts() {
    }

    /** Re-renders the charts from a saved {@code results.csv}: {@code Charts <dir>}. */
    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args.length > 0 ? args[0] : "docs/bench");
        List<LoadGenerator.Result> results = Files.readAllLines(dir.resolve("results.csv")).stream()
                .skip(1)
                .filter(line -> !line.isBlank())
                .map(line -> line.split(","))
                .map(f -> new LoadGenerator.Result(Integer.parseInt(f[0]), Integer.parseInt(f[1]),
                        Long.parseLong(f[2]), Double.parseDouble(f[3]), Double.parseDouble(f[4]),
                        Double.parseDouble(f[5]), Double.parseDouble(f[6]), Double.parseDouble(f[7]),
                        Double.parseDouble(f[8]), Long.parseLong(f[9])))
                .toList();
        render(results, dir);
    }

    static void render(List<LoadGenerator.Result> results, Path out) throws IOException {
        Files.writeString(out.resolve("throughput.svg"), chart(results,
                "Write throughput vs. concurrent clients",
                "Committed PUTs per second, fsync on, all nodes on one machine",
                "ops/s", LoadGenerator.Result::throughput, false));
        Files.writeString(out.resolve("latency-p99.svg"), chart(results,
                "p99 write latency vs. concurrent clients",
                "99th percentile, client-measured (includes retries and redirects)",
                "ms", LoadGenerator.Result::p99Ms, true));
    }

    private static String chart(List<LoadGenerator.Result> results, String title, String subtitle, String unit,
                                ToDoubleFunction<LoadGenerator.Result> metric, boolean decimals) {
        Map<Integer, List<LoadGenerator.Result>> bySize = results.stream()
                .collect(Collectors.groupingBy(LoadGenerator.Result::nodes, TreeMap::new, Collectors.toList()));
        List<Integer> clients = results.stream().map(LoadGenerator.Result::clients).distinct().sorted().toList();
        double max = results.stream().mapToDouble(metric).max().orElse(1);
        double[] ticks = niceTicks(max);
        double yMax = ticks[ticks.length - 1];

        int plotW = WIDTH - LEFT - RIGHT;
        int plotH = HEIGHT - TOP - BOTTOM;
        StringBuilder svg = new StringBuilder();
        svg.append(String.format(Locale.ROOT,
                "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 %d %d\" width=\"%d\" height=\"%d\" "
                        + "role=\"img\" aria-label=\"%s\" font-family=\"-apple-system, Segoe UI, Helvetica, Arial, "
                        + "sans-serif\">%n", WIDTH, HEIGHT, WIDTH, HEIGHT, esc(title)));
        svg.append("""
                <style>
                  .bg { fill: #fcfcfb; } .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .t3 { fill: #6f6e69; }
                  .grid { stroke: #e7e6e2; } .axis { stroke: #b9b8b2; }
                  .s1 { stroke: #2a78d6; fill: #2a78d6; } .s2 { stroke: #eb6834; fill: #eb6834; }
                  .s3 { stroke: #1baf7a; fill: #1baf7a; } .ring { stroke: #fcfcfb; }
                  @media (prefers-color-scheme: dark) {
                    .bg { fill: #1a1a19; } .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; } .t3 { fill: #a3a29a; }
                    .grid { stroke: #2f2f2d; } .axis { stroke: #57564f; }
                    .s1 { stroke: #3987e5; fill: #3987e5; } .s2 { stroke: #d95926; fill: #d95926; }
                    .s3 { stroke: #199e70; fill: #199e70; } .ring { stroke: #1a1a19; }
                  }
                </style>
                """);
        svg.append(String.format(Locale.ROOT, "<rect class=\"bg\" width=\"%d\" height=\"%d\" rx=\"8\"/>%n", WIDTH, HEIGHT));
        svg.append(String.format(Locale.ROOT,
                "<text class=\"t1\" x=\"%d\" y=\"30\" font-size=\"17\" font-weight=\"600\">%s</text>%n", LEFT, esc(title)));
        svg.append(String.format(Locale.ROOT,
                "<text class=\"t2\" x=\"%d\" y=\"50\" font-size=\"12.5\">%s</text>%n", LEFT, esc(subtitle)));

        // legend: one row, above the plot
        int lx = LEFT;
        int s = 0;
        for (int nodes : bySize.keySet()) {
            String cls = SERIES_CLASS[s++ % SERIES_CLASS.length];
            svg.append(String.format(Locale.ROOT,
                    "<line class=\"%s\" x1=\"%d\" y1=\"68\" x2=\"%d\" y2=\"68\" stroke-width=\"2\"/>"
                            + "<circle class=\"%s\" cx=\"%d\" cy=\"68\" r=\"4\"/>%n", cls, lx, lx + 18, cls, lx + 9));
            svg.append(String.format(Locale.ROOT,
                    "<text class=\"t2\" x=\"%d\" y=\"72\" font-size=\"12\">%d nodes</text>%n", lx + 24, nodes));
            lx += 96;
        }

        // gridlines + y ticks (recessive)
        for (double tick : ticks) {
            double y = TOP + plotH - tick / yMax * plotH;
            svg.append(String.format(Locale.ROOT,
                    "<line class=\"grid\" x1=\"%d\" y1=\"%.1f\" x2=\"%d\" y2=\"%.1f\" stroke-width=\"1\"/>%n",
                    LEFT, y, LEFT + plotW, y));
            svg.append(String.format(Locale.ROOT,
                    "<text class=\"t3\" x=\"%d\" y=\"%.1f\" font-size=\"11\" text-anchor=\"end\">%s</text>%n",
                    LEFT - 8, y + 4, compact(tick, decimals)));
        }
        svg.append(String.format(Locale.ROOT,
                "<text class=\"t3\" x=\"%d\" y=\"%d\" font-size=\"11\" text-anchor=\"end\">%s</text>%n",
                LEFT - 8, TOP - 16, esc(unit)));

        // x axis: clients, evenly spaced (log2)
        svg.append(String.format(Locale.ROOT,
                "<line class=\"axis\" x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" stroke-width=\"1\"/>%n",
                LEFT, TOP + plotH, LEFT + plotW, TOP + plotH));
        for (int i = 0; i < clients.size(); i++) {
            double x = xAt(i, clients.size(), plotW);
            svg.append(String.format(Locale.ROOT,
                    "<text class=\"t3\" x=\"%.1f\" y=\"%d\" font-size=\"11\" text-anchor=\"middle\">%d</text>%n",
                    x, TOP + plotH + 18, clients.get(i)));
        }
        svg.append(String.format(Locale.ROOT,
                "<text class=\"t2\" x=\"%d\" y=\"%d\" font-size=\"12\" text-anchor=\"middle\">concurrent clients</text>%n",
                LEFT + plotW / 2, HEIGHT - 14));

        // series: 2px lines, 8px markers with a 2px surface ring, direct label at the last point
        List<double[]> endLabels = new java.util.ArrayList<>(); // {y, nodes}
        s = 0;
        for (var entry : bySize.entrySet()) {
            String cls = SERIES_CLASS[s++ % SERIES_CLASS.length];
            List<LoadGenerator.Result> points = entry.getValue();
            StringBuilder path = new StringBuilder();
            double lastX = 0;
            double lastY = 0;
            for (LoadGenerator.Result r : points) {
                double x = xAt(clients.indexOf(r.clients()), clients.size(), plotW);
                double y = TOP + plotH - metric.applyAsDouble(r) / yMax * plotH;
                path.append(path.isEmpty() ? "M" : " L").append(String.format(Locale.ROOT, "%.1f %.1f", x, y));
                lastX = x;
                lastY = y;
            }
            svg.append(String.format(Locale.ROOT,
                    "<path class=\"%s\" d=\"%s\" fill=\"none\" stroke-width=\"2\" stroke-linejoin=\"round\" "
                            + "style=\"fill: none\"/>%n", cls, path));
            for (LoadGenerator.Result r : points) {
                double x = xAt(clients.indexOf(r.clients()), clients.size(), plotW);
                double y = TOP + plotH - metric.applyAsDouble(r) / yMax * plotH;
                svg.append(String.format(Locale.ROOT,
                        "<circle class=\"%s\" cx=\"%.1f\" cy=\"%.1f\" r=\"4\"/>"
                                + "<circle class=\"ring\" cx=\"%.1f\" cy=\"%.1f\" r=\"5\" fill=\"none\" stroke-width=\"2\"/>"
                                + "<title>%d nodes, %d clients: %s %s</title>%n",
                        cls, x, y, x, y, entry.getKey(), r.clients(),
                        compact(metric.applyAsDouble(r), true), esc(unit)));
            }
            endLabels.add(new double[] {lastY, entry.getKey(), lastX});
        }
        // direct labels must not collide: keep at least 15px between them, in value order
        endLabels.sort(java.util.Comparator.comparingDouble(l -> l[0]));
        for (int i = 1; i < endLabels.size(); i++) {
            endLabels.get(i)[0] = Math.max(endLabels.get(i)[0], endLabels.get(i - 1)[0] + 15);
        }
        for (double[] label : endLabels) {
            svg.append(String.format(Locale.ROOT,
                    "<text class=\"t1\" x=\"%.1f\" y=\"%.1f\" font-size=\"12\">%d nodes</text>%n",
                    label[2] + 10, label[0] + 4, (int) label[1]));
        }
        svg.append("</svg>\n");
        return svg.toString();
    }

    private static double xAt(int i, int count, int plotW) {
        int pad = 18;
        return LEFT + pad + (count == 1 ? 0 : (double) i / (count - 1) * (plotW - 2 * pad));
    }

    /** 0 plus 4-6 round ticks covering {@code max}. */
    static double[] niceTicks(double max) {
        if (max <= 0) {
            return new double[] {0, 1};
        }
        double rough = max / 5;
        double magnitude = Math.pow(10, Math.floor(Math.log10(rough)));
        double step = magnitude;
        for (double m : new double[] {1, 2, 2.5, 5, 10}) {
            if (m * magnitude >= rough) {
                step = m * magnitude;
                break;
            }
        }
        int n = (int) Math.ceil(max / step);
        double[] ticks = new double[n + 1];
        for (int i = 0; i <= n; i++) {
            ticks[i] = i * step;
        }
        return ticks;
    }

    static String compact(double v, boolean decimals) {
        if (v >= 1000) {
            double k = v / 1000;
            return (k == Math.rint(k) ? String.format(Locale.ROOT, "%.0fK", k) : String.format(Locale.ROOT, "%.1fK", k));
        }
        if (decimals && v != Math.rint(v)) {
            return String.format(Locale.ROOT, v < 10 ? "%.1f" : "%.0f", v);
        }
        return String.format(Locale.ROOT, "%.0f", v);
    }

    private static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}

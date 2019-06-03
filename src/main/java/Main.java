import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        final File file = getFile(args);
        if (file == null) {
            System.err.println("Given file doesn't exist");
            return;
        }
        final SearchRequest searchRequest = buildSearchRequest(args);

        final List<SearchProcessorResult> results = executeSearchTasks(file, searchRequest);
        results.stream().flatMap(searchProcessorResult -> {
            final Long processorStartLine = results.stream()
                    .filter(result -> result.getProcessorIndex() < searchProcessorResult.getProcessorIndex())
                    .map(SearchProcessorResult::getProcessedLinesCount)
                    .reduce(Long::sum)
                    .orElse(0L);
            return searchProcessorResult.getResults().stream().map(searchResult -> {
                final long lineNumber = processorStartLine + searchResult.getLineNumber();
                final SearchResultView searchResultView = new SearchResultView(searchResult.getLine(), lineNumber);
                searchResultView.underline(searchResult.getTokenIndices());
                return searchResultView;

            });
        })
                .sorted(Comparator.comparing(SearchResultView::getLineNumber))
                .forEach(System.out::println);
    }

    private static List<SearchProcessorResult> executeSearchTasks(final File file, final SearchRequest searchRequest) throws InterruptedException, ExecutionException, IOException {
        final int chunks = Runtime.getRuntime().availableProcessors();
        final ExecutorService executorService = Executors.newFixedThreadPool(chunks);
        final ExecutorCompletionService<SearchProcessorResult> completionService = new ExecutorCompletionService<>(executorService);
        final long[] offsets = calculateOffsets(chunks, file);
        final List<SearchProcessorResult> results = new ArrayList<>();
        for (int i = 0; i < chunks; i++) {
            final long start = offsets[i];
            final long end = i < chunks - 1 ? offsets[i + 1] : file.length();
            completionService.submit(new SearchProcessor.Builder()
                    .withFile(file)
                    .withStart(start)
                    .withEnd(end)
                    .withPattern(searchRequest)
                    .withIndex(i)
                    .build());
        }
        while (results.size() < chunks) {
            final Future<SearchProcessorResult> future = completionService.take();
            if (future.isDone()) {
                results.add(future.get());
            }
        }
        executorService.shutdown();
        return results;
    }

    private static long[] calculateOffsets(final int chunks, final File file) throws IOException {
        final long[] offsets = new long[chunks];
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int i = 1; i < chunks; i++) {
                raf.seek(i * file.length() / chunks);
                while (true) {
                    final int read = raf.read();
                    if (read == '\n' || read == -1) {
                        break;
                    }
                }
                offsets[i] = raf.getFilePointer();
            }
            return offsets;
        }
    }

    private static File getFile(String[] args) throws URISyntaxException {
        File file;
        if (args.length == 0) {
            URL resource = Main.class.getResource("/Jabberwocky.txt");
            file = new File(resource.toURI());
        } else {
            file = new File(args[1]);
        }
        if (!file.exists()) {
            return null;
        }
        return file;
    }

    private static SearchRequest buildSearchRequest(String[] args) {
        final String searchTerm = args.length == 0 ? "All" : args[0];
        final boolean fuzzySearch = args.length >= 3 && "part".equals(args[2]);
        final boolean caseSensitive = args.length >= 4 && "ci".equals(args[3]);
        return new SearchRequest.Builder()
                .withSearchTerm(searchTerm)
                .withCaseSensitive(caseSensitive)
                .withFuzzySearch(fuzzySearch)
                .build();
    }

    static class SearchProcessor implements Callable<SearchProcessorResult> {
        private final File file;
        private final long start;
        private final long end;
        private final int processorIndex;
        private final SearchRequest searchRequest;
        private long lineCounter;

        private SearchProcessor(final File file,
                                final long start,
                                final long end,
                                final int processorIndex,
                                final SearchRequest searchRequest) {
            this.file = file;
            this.start = start;
            this.end = end;
            this.processorIndex = processorIndex;
            this.searchRequest = searchRequest;
        }

        public SearchProcessorResult call() throws IOException {
            final SearchProcessorResult result = new SearchProcessorResult(processorIndex);
            Pattern mainPattern = searchRequest.isFuzzySearch() ? searchRequest.getFuzzyPattern() : searchRequest.getPattern();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(start);
                while (raf.getFilePointer() < end) {
                    String line = raf.readLine();
                    if (line == null) {
                        continue;
                    }
                    lineCounter++;

                    final Matcher matcher = mainPattern.matcher(line);
                    if (matcher.find()) {
                        final SearchResult searchResult = new SearchResult(line, lineCounter);
                        final Matcher strictMatcher = searchRequest.getPattern().matcher(line);
                        while (strictMatcher.find()) {
                            searchResult.addTokenIndex(new TokenIndex(strictMatcher.start(), strictMatcher.end()));
                        }
                        result.addSearchResult(searchResult);
                    }
                }
                result.setProcessedLinesCount(lineCounter);
            }
            return result;
        }

        static class Builder {
            private File file;
            private long start;
            private long end;
            private int index;
            private SearchRequest searchRequest;

            Builder withFile(File file) {
                this.file = file;
                return this;
            }

            Builder withStart(long start) {
                this.start = start;
                return this;
            }

            Builder withEnd(long end) {
                this.end = end;
                return this;
            }

            Builder withIndex(int index) {
                this.index = index;
                return this;
            }

            Builder withPattern(SearchRequest searchRequest) {
                this.searchRequest = searchRequest;
                return this;
            }

            Main.SearchProcessor build() {
                return new Main.SearchProcessor(file, start, end, index, searchRequest);
            }
        }
    }
}

class SearchRequest {
    private final boolean fuzzySearch;
    private final boolean caseSensitive;
    private final Pattern fuzzyPattern;
    private final Pattern pattern;

    private SearchRequest(final boolean fuzzySearch, final boolean caseSensitive, final Pattern fuzzyPattern, final Pattern pattern) {
        this.fuzzySearch = fuzzySearch;
        this.caseSensitive = caseSensitive;
        this.fuzzyPattern = fuzzyPattern;
        this.pattern = pattern;
    }

    boolean isFuzzySearch() {
        return fuzzySearch;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    Pattern getFuzzyPattern() {
        return fuzzyPattern;
    }

    Pattern getPattern() {
        return pattern;
    }

    static class Builder {
        private String searchTerm;
        private boolean fuzzySearch;
        private boolean caseSensitive;

        Builder withSearchTerm(String searchTerm) {
            this.searchTerm = searchTerm;
            return this;
        }

        Builder withFuzzySearch(boolean fuzzySearch) {
            this.fuzzySearch = fuzzySearch;
            return this;
        }

        Builder withCaseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
            return this;
        }

        SearchRequest build() {
            final Pattern fuzzyPattern = Pattern.compile(".*\\b" + searchTerm + "\\b.*");
            final Pattern pattern = Pattern.compile("\\b" + searchTerm + "\\b");
            return new SearchRequest(fuzzySearch, caseSensitive, fuzzyPattern, pattern);
        }
    }
}


class SearchProcessorResult {
    private int processorIndex;
    private long processedLinesCount;
    private List<SearchResult> results = new ArrayList<>();


    int getProcessorIndex() {
        return processorIndex;
    }

    void addSearchResult(SearchResult searchResult) {
        results.add(searchResult);
    }

    SearchProcessorResult(int processorIndex) {
        this.processorIndex = processorIndex;
    }

    List<SearchResult> getResults() {
        return results;
    }

    long getProcessedLinesCount() {
        return processedLinesCount;
    }

    void setProcessedLinesCount(long processedLinesCount) {
        this.processedLinesCount = processedLinesCount;
    }
}

class SearchResult {
    private String line;
    private long lineNumber;
    private List<TokenIndex> tokenIndices = new ArrayList<>();

    SearchResult(String line, long lineNumber) {
        this.line = line;
        this.lineNumber = lineNumber;
    }

    String getLine() {
        return line;
    }

    long getLineNumber() {
        return lineNumber;
    }

    List<TokenIndex> getTokenIndices() {
        return tokenIndices;
    }

    void addTokenIndex(final TokenIndex tokenIndex) {
        this.tokenIndices.add(tokenIndex);
    }
}

class TokenIndex {
    private final int start;
    private final int end;

    TokenIndex(int start, int end) {
        this.start = start;
        this.end = end;
    }

    boolean isBetween(int index) {
        return index >= start && index <= end;
    }
}

class SearchResultView {

    private final String line;
    private final long lineNumber;
    private String tokenMarker;

    SearchResultView(final String line, final long lineNumber) {
        this.line = line;
        this.lineNumber = lineNumber;
    }

    long getLineNumber() {
        return lineNumber;
    }

    void underline(List<TokenIndex> tokenIndices) {
        final StringBuilder marker = new StringBuilder();
        for (int i = 0; i <= (int) (Math.log10(lineNumber) + 1); i++) {
            marker.append(" ");
        }
        for (int i = 0; i < line.length(); i++) {
            final int index = i;
            if (tokenIndices.stream().anyMatch(tokenIndex -> tokenIndex.isBetween(index))) {
                marker.append("^");
            } else {
                marker.append("_");

            }
        }
        tokenMarker = marker.toString();
    }

    @Override
    public String toString() {
        return lineNumber + " " + line + "\n" + tokenMarker;
    }
}
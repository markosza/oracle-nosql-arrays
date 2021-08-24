
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;

/**
 * A program to demostrate the writing and optimizing of queries over data
 * containing nested arrays.
 *
 * The program uses the Oracle NoSQL java driver and runs over cloudsim, which
 * is a single-process simulator of a cloud-based Oracle NoSQL installation.
 *
 * The program creates a table whose rows model users of a show-streaming
 * service. It creates 5 indexes over this table, and loads the table with
 * some sample rows. The sample rows are contained in a number of .json files
 * (one file per row). Four sample rows are provided, but users can add their
 * own .json files as well. Finally, the program allows its users to run a
 * set of pre-packaged queries, or to run a specific query from this set, or
 * to write and execute their own query provided as a command-line option.
 *
 * Available program options are:
 *
 * -showPlan
 * Use it if you want to display the execution plan of the query or queries
 *
 * -qno N
 * N is an integer number between 1 and 12 that specifies which of the 12
 * pre-packaged queries will be executed by the program
 *
 * -query "<queryText>"
 * The program will execute the query provided by <queryText>. If the query
 * text contains any string literals, they must be written using single quotes. 
 */
public class NestedArraysDemo {

    private static String tableName = "users";

    /*
     * DDL commands
     */
    private static String tableDDL = 
        "create table if not exists users(" +
        "    acct_id integer,"      +
        "    user_id integer,"      + 
        "    info json,"           +
        "    primary key(acct_id, user_id))";

    private static String idx_country_showid_date_ddl =
        "create index if not exists idx_country_showid_date on users(" +
        "    info.country as string, " +
        "    info.shows[].showId as integer, " +
        "    info.shows[].seriesInfo[].episodes[].date as string) ";

    private static String idx_country_genre_ddl =
        "create index if not exists idx_country_genre on users(" +
        "    info.country as string, " +
        "    info.shows[].genres[] as string)";

    private static String idx_showid_ddl =
        "create index if not exists idx_showid on users( " +
        "    info.shows[].showId as integer) " +
        "    with unique keys per row";

    private static String idx_showid_minWatched_ddl =
        "create index if not exists idx_showid_minWatched on users( " + 
        "    info.shows[].showId as integer, " +
        "    info.shows[].seriesInfo[].episodes[].minWatched as integer, " +
        "    info.shows[].seriesInfo[].episodes[].episodeID as integer) " +
        "    with unique keys per row";

    private static String idx_showid_seasonNum_minWatched_ddl =
        "create index if not exists idx_showid_seasonNum_minWatched on users(" +
        "    info.shows[].showId as integer," +
        "    info.shows[].seriesInfo[].seasonNum as integer," +
        "    info.shows[].seriesInfo[].episodes[].minWatched as integer, " +
        "    info.shows[].seriesInfo[].episodes[].episodeID as integer) " +
        "with unique keys per row";

    /*
     * Sample queries
     */
    private static String queries[] = { 

    /*
     * Q1:
     * Return the number of users in USA that have shown an interest
     * in the show with id 16. 
     *
     * Result:
     * {"cnt":2}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where u.info.country = \"USA\" and " +
    "      u.info.shows.showId =any 16",

    /*
     * Q2:
     * Return the number of users in USA that have watched at least one
     * episode of show 16 after  2021-04-01.
     *
     * Result:
     * {"cnt":1}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where u.info.country = \"USA\" and " +
    "      exists u.info.shows[$element.showId = 16]." +
    "             seriesInfo.episodes[$element.date > \"2021-04-01\"]",

    /*
     * Q3:
     * Return the number of users in USA that have watched at least one
     * episode of show 16 and have also watched an episode of some show
     * after  2021-04-01.
     *
     * Result:
     * {"cnt":2}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where u.info.country = \"USA\" and " +
    "      u.info.shows.showId =any 16 and " +
    "      u.info.shows.seriesInfo.episodes.date >any \"2021-04-01\"",

    /*
     * Q4:
     * Return the number of users that have watched at least one episode of
     * show 15 after 2021-04-01.
     *
     * Result:
     * {"cnt":3}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where exists u.info.shows[$element.showId = 15]." +
    "             seriesInfo.episodes[$element.date > \"2021-04-01\"]",

    /*
     * Q5:
     * Same as Q4, but forcing the use of index idx_country_showid_date
     *
     * Result:
     * {"cnt":3}
     */
    "select /*+ FORCE_INDEX(users idx_country_showid_date) */ " +
    "       count(*) as cnt " +
    "from users u " +
    "where exists u.info.shows[$element.showId = 15]." +
    "             seriesInfo.episodes[$element.date > \"2021-04-01\"]",

    /*
     * Q6:
     * Return the number of users in USA that have watched a French or Danish
     * show in 2021.
     *
     * Result:
     * {"cnt":2}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where u.info.country = \"USA\" and " +
    "      exists u.info.shows[ " +
    "      exists $element.genres[$element in (\"french\", \"danish\")] and " +
    "      exists $element.seriesInfo.episodes[\"2021-01-01\" <= $element.date and " +
    "                                          $element.date <= \"2021-12-31\"] " +
    "      ]" ,
    
    /*
     * Q7:
     * For each user in USA that has watched at least one episode of show 16
     * after 2021-04-01, return his/her first and last name, the total time
     * the user has spent watching show 16, and an array containing information
     * about all the episodes of show 16 that the user has watched. Specifically,
     * each element of this array is a json document containing the show name, the
     * season number, the episode id, and the date the episode was watched.
     *
     * Result:
     * { "acct_id":2,"user_id":1,
     *   "time":220,
     *   "episodes":[
     *      {"dateWatched":"2021-03-18","episodeId":20,"seasonNum":1,"showName":"Rita"},
     *      {"dateWatched":"2021-03-19","episodeId":30,"seasonNum":1,"showName":"Rita"},
     *      {"dateWatched":"2021-05-05","episodeId":40,"seasonNum":2,"showName":"Rita"},
     *      {"dateWatched":"2021-05-06","episodeId":50,"seasonNum":2,"showName":"Rita"}
     *   ]
     * }
     */
    "select u.acct_id, u.user_id, " +
    "       seq_sum(u.info.shows[$element.showId = 16].seriesInfo.episodes.minWatched) as time, " +
    "       [ seq_transform(u.info.shows[$element.showId = 16], " +
    "                       seq_transform($sq1.seriesInfo[], " +
    "                                     seq_transform($sq2.episodes[], " +
    "                                     { \"showName\" : $sq1.showName, " +
    "                                       \"seasonNum\" : $sq2.seasonNum, " +
    "                                       \"episodeId\" : $sq3.episodeID, " +
    "                                       \"dateWatched\" : $sq3.date " +
    "                                     }))) ] as episodes " +
    "from users u " +
    "where u.info.country = \"USA\" and " +
    "      exists u.info.shows[$element.showId = 16]." +
    "             seriesInfo.episodes[$element.date > \"2021-04-01\"]",

    /*
     * Q8:
     * Return the number of users who have fully watched show 15 (all seasons
     * and all episodes to their full length).
     *
     * Result:
     * {"cnt":1}
     */
    "select count(*) as cnt " +
    "from users u " +
    "where u.info.shows.showId =any 15 and " +
    "      size(u.info.shows[$element.showId = 15].seriesInfo) = " +
    "      u.info.shows[$element.showId = 15].numSeasons and " +
    "      not seq_transform(u.info.shows[$element.showId = 15].seriesInfo[], " +
    "                        $sq1.numEpisodes = size($sq1.episodes)) =any false and " +
    "      not seq_transform(u.info.shows[$element.showId = 15].seriesInfo.episodes[]," +
    "                        $sq1.lengthMin = $sq1.minWatched) =any false",

    /*
     * Q9:
     * For each user in USA that has watched at least one episode of show 16
     * after 2021-04-01, return one result for every episode of show 16 that
     * the user has watch. Each such result contains the user's first and
     * last name, the show name, the season number, the episode id, and the
     * date the episode was watched.
     *
     * Result:
     * {"acct_id":2,"user_id":1,"showName":"Rita","seasonNum":1,"episodeID":20,"date":"2021-03-18"}
     * {"acct_id":2,"user_id":1,"showName":"Rita","seasonNum":1,"episodeID":30,"date":"2021-03-19"}
     * {"acct_id":2,"user_id":1,"showName":"Rita","seasonNum":2,"episodeID":40,"date":"2021-05-05"}
     * {"acct_id":2,"user_id":1,"showName":"Rita","seasonNum":2,"episodeID":50,"date":"2021-05-06"}
     *
     */
    "select u.acct_id, u.user_id, " +
    "       $show.showName, $season.seasonNum, " +
    "       $episode.episodeID, $episode.date " +    
    "from users u, u.info.shows[] as $show, " +
    "              $show.seriesInfo[] as $season, " +
    "              $season.episodes[] as $episode " +
    "where u.info.country = \"USA\" and " +
    "      $show.showId = 16 and " +
    "      $show.seriesInfo.episodes.date >any \"2021-04-01\"",

    /*
     * Q10:
     * For each show, return the number of users who have shown an interest in
     * that show. Return the results ordered by the number of users in
     * descending order.
     *
     * Result:
     * {"showId":15,"cnt":4}
     * {"showId":16,"cnt":2}
     * {"showId":26,"cnt":1}
     */
    "select $show.showId, count(*) as cnt " +
    "from users u, unnest(u.info.shows[] as $show) " +
    "group by $show.showId " +
    "order by count(*) desc",

    /*
     * Q11:
     * For each show, return the total time users have spent watching that show.
     * Return the results ordered by that time in descending order.
     *
     * Result:
     * {"showId":15,"totalTime":642}
     * {"showId":16,"totalTime":440}
     * {"showId":26,"totalTime":225}
     */
    "select $show.showId, sum($show.seriesInfo.episodes.minWatched) as totalTime " +
    "from users u, unnest(u.info.shows[] as $show) " +
    "group by $show.showId " +
    "order by sum($show.seriesInfo.episodes.minWatched) desc",

    /*
     * Q12:
     * Same as Q11, but uses index idx_showid
     */
    "select $show.showId, " +
    "       sum(u.info.shows[$element.showId = $show.showId]." +
    "       seriesInfo.episodes.minWatched) as totalTime " +
    "from users u, unnest(u.info.shows[] as $show) " +
    "group by $show.showId " +
    "order by sum(u.info.shows[$element.showId = $show.showId]." +
    "             seriesInfo.episodes.minWatched) desc",

    /*
     * Q13:
     * For each show and associated season, return the total time users have
     * spent watching that show and season. Return the results ordered by that
     * time in descending order.
     *
     * Result:
     * {"showId":15,"totalTime":347,"seasonNum":1}
     * {"showId":15,"totalTime":295,"seasonNum":2}
     * {"showId":16,"totalTime":250,"seasonNum":1}
     * {"showId":16,"totalTime":190,"seasonNum":2}
     * {"showId":26,"totalTime":145,"seasonNum":1}
     * {"showId":26,"totalTime":80,"seasonNum":2}
     */
    "select $show.showId, " +
    "       $seriesInfo.seasonNum, " +
    "       sum($seriesInfo.episodes.minWatched) as totalTime " +
    "from users u, unnest(u.info.shows[] as $show, " +
    "                       $show.seriesInfo[] as $seriesInfo) " +
    "group by $show.showId, $seriesInfo.seasonNum " +
    "order by sum($seriesInfo.episodes.minWatched) desc"
    };

    private static int qno = -1;

    private static String queryText;

    private static boolean showPlan;

    private static String endpoint;

    private static NoSQLHandle handle;

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            usage();
            return;
        }

        endpoint = args[0];

        for (int i = 1; i < args.length; ++i) {
            String arg = args[i];

            if (arg.equals("-showPlan")) {
                showPlan = true;
            } else if (arg.equals("-qno")) {
                ++i;
                qno = Integer.parseInt(args[i]) - 1;

                if (qno < 0 || qno >= queries.length) {
                    System.out.println("Invalid query number");
                    return;
                }
            } else if (arg.equals("-query")) {
                ++i;
                queryText = args[i];
            }
        }

        if (queryText != null && qno >= 0) {
            qno = -1;
        }

        /* Get a handle to NoSQL DB */
        handle = getHandle();

        try {
            /* Create the users table */
            TableRequest tableRequest = new TableRequest();
            tableRequest.setStatement(tableDDL);
            tableRequest.setTableLimits(new TableLimits(50, 50, 50));
            //System.out.println("Creating table " + tableName);

            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created table " + tableName);

            /* Create index idx_country_showid_date */
            tableRequest = new TableRequest();
            tableRequest.setStatement(idx_country_showid_date_ddl);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx_country_showid_date");

            /* Create index idx_showid */
            tableRequest.setStatement(idx_showid_ddl);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx_showid");

            /* Create index idx_showid_minWatched */
            tableRequest.setStatement(idx_showid_minWatched_ddl);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx_showid_minWatched");

            /* Create index idx_showid_seasonNum_minWatched */
            tableRequest.setStatement(idx_showid_seasonNum_minWatched_ddl);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx_showid_seasonNum_minWatched");

            /* Create index idx_country_genre */
            tableRequest = new TableRequest();
            tableRequest.setStatement(idx_country_genre_ddl);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx_country_genre");

            /* Insert rows in the table */
            File currentDir = new File(System.getProperty("user.dir"));
            File[] files = currentDir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    String fname = file.getName();
                    if (fname.endsWith(".json")) {
                        insertRow(fname);
                    }
                }
            }

            /* Run queries */
            if (queryText != null) {
                runQuery(-1, queryText, showPlan);
            } else if (qno >= 0) {
                runQuery(qno, null, showPlan);
            } else {
                for (int i = 0; i < queries.length; ++i) {
                    runQuery(i, null, showPlan);
                }
            }

            /* DROP the table */
            System.out.println("Dropping table " + tableName);
            tableRequest = new TableRequest();
            tableRequest.setStatement("DROP TABLE IF EXISTS " + tableName);
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
        } catch (Exception e) {
            System.err.println("Problem seen: " + e);
        } finally {
            /* Shutdown handle so process can exit */
            handle.close();
        }
    }

    public static void runQuery(int qid, String queryText, boolean showPlan) {

        if (qid >= 0) {
            System.out.println("Executing query " + (qid + 1) + "\n");
            queryText = queries[qid];
        }

        /*
         * A prepared statement is used as it is the most efficient
         * way to handle queries that are run multiple times.
         */
        PrepareRequest prepReq = new PrepareRequest();
        prepReq.setStatement(queryText);
        if (showPlan) {
            prepReq.setGetQueryPlan(true);
        }
        PrepareResult prepRes = handle.prepare(prepReq);
        PreparedStatement prepStmt = prepRes.getPreparedStatement();
        if (showPlan) {
            System.out.println("\nQuery plan:");
            System.out.println(prepStmt.getQueryPlan() + "\n");
        }

        QueryRequest queryReq = new QueryRequest();
        queryReq.setPreparedStatement(prepStmt);

        try {
            do {
                QueryResult queryRes = handle.query(queryReq);
                List<MapValue> results = queryRes.getResults();
                for (MapValue res : results) {
                    System.out.println(res + "\n");
                }
            } while (!queryReq.isDone());
        } catch (ReadThrottlingException rte) {
            /*
             * Applications need to be able to handle throttling exceptions.
             * The default retry handler may retry these internally. This
             * can result in slow performance. It is best if an application
             * rate-limits itself to stay under a table's read and write
             * limits.
             */
        }
    }

    private static void insertRow(String fname)
        throws IOException {

        //System.out.println("Inserting row from file " + fname);

        String jsonText = readFileToString(fname);
        MapValue row = (MapValue)JsonUtils.createValueFromJson(jsonText, null);
        PutRequest putRequest = new PutRequest();
        putRequest.setValue(row);
        putRequest.setTableName(tableName);
        PutResult putRes = handle.put(putRequest);

        System.out.println("Inserted row from file " + fname);
    }

    private static String readFileToString(String fname) throws IOException {

        BufferedReader bufferedReader =
            new BufferedReader(new FileReader(fname));

        StringBuffer stringBuffer = new StringBuffer();
        String line = null;

        while ((line = bufferedReader.readLine()) != null) {
            stringBuffer.append(line).append("\n");
        }

        bufferedReader.close();
        return stringBuffer.toString();
    }

    private static NoSQLHandle getHandle() {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        config.setAuthorizationProvider(CloudSimProvider.getProvider());
        return NoSQLHandleFactory.createNoSQLHandle(config);
    }

    private static void usage() {
        System.err.println("Usage: java NestedArraysDemo <endpoint> " +
                           "[-qno <n> | -query <query text>] [-showPlan]");
        System.err.println("For example: java NestedArraysDemo " +
                           "http://localhost:8080 -showPlan");
    }

    /**
     * A simple provider that uses a manufactured id for use by the
     * Cloud Simulator. It is used as a namespace for tables and not
     * for any actual authorization or authentication.
     */
    private static class CloudSimProvider implements AuthorizationProvider {

        private static final String id = "Bearer exampleId";

        private static AuthorizationProvider provider =
            new CloudSimProvider();

        private static AuthorizationProvider getProvider() {
            return provider;
        }

        /**
         * Disallow external construction. This is a singleton.
         */
        private CloudSimProvider() {}

        @Override
        public String getAuthorizationString(Request request) {
            return id;
        }

        @Override
        public void close() {}
    }
}

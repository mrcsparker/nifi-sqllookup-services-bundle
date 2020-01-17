/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mrcsparker.nifi.sqllookup;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.junit.BeforeClass;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class AbstractSQLLookupServiceTest {

    private final static String DB_LOCATION = "target/db";

    TestRunner runner;

    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    void setupDB() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        if (dbLocation.exists()) {
            dbLocation.delete();
        }


        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcpService")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table IF EXISTS TEST_LOOKUP_DB");
        } catch (final SQLException sqle) {
            sqle.printStackTrace();
        }

        stmt.execute("CREATE TABLE IF NOT EXISTS TEST_LOOKUP_DB " +
                "( " +
                "    id serial not null constraint test_lookup_db_pk primary key, " +
                "    name VARCHAR(30), " +
                "    value VARCHAR(255), " +
                "    period INT DEFAULT 1, " +
                "    address VARCHAR(255), " +
                "    price FLOAT(52) DEFAULT 0.00 " +
                ")");

        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('495304346258559', 'Wildfire at Midnight', 7, '94384 Stroman Pike', 48.66)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('456148015917293', 'The Wealth of Nations', 9, '3743 Amanda Mountain', 359.92)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('526924199146123', 'All Passion Spent', 3, '107 Alaina Row', 256.77)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('860683959429897', 'Shall not Perish', 7, '232 Konopelski Plain', 23.16)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('528661513839698', 'A Handful of Dust', 6, '48538 Leuschke Ways', 401.83)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('355663598958946', 'The Glory and the Dream', 7, '211 Nitzsche Ports', 132.36)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('911753660676323', 'Look to Windward', 2, '56834 Stroman Bridge', 42.48)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('997417069743624', 'All Passion Spent', 7, '2359 Chauncey Street', 444.41)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('986873446696583', 'The Glory and the Dream', 9, '613 Nikolaus Lodge', 419.46)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('990409804141864', 'Recalled to Life', 4, '85463 Guy Courts', 178.04)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('011340994294624', 'Clouds of Witness', 7, '973 Quigley Hill', 404.92)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('547153353139832', 'Quo Vadis', 7, '21193 Keegan Rapid', 83.58)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('993245649723970', 'From Here to Eternity', 2, '44027 Howe Drives', 435.61)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('528752270770869', 'The Needles Eye', 4, '5241 Green Mills', 226.61)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('357694592870637', 'The House of Mirth', 9, '7736 Langworth Pines', 281.27)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('459447622180312', 'No Highway', 9, '6871 Ortiz Dale', 111.97)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('013868509203308', 'Sleep the Brave', 2, '875 Wilderman Rue', 54.38)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('531837644973318', 'Cabbages and Kings', 7, '8505 Donnelly Ranch', 187.4)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('011593966480635', 'In a Glass Darkly', 7, '4816 Collier River', 218.38)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('531195924901818', 'Noli Me Tangere', 9, '863 Windler Station', 105.07)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('333262659722965', 'Tiger! Tiger!', 6, '1127 Cora Creek', 202.89)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('302255998921634', 'Absalom, Absalom!', 10, '238 Barrows Plains', 399.92)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('107540410966037', 'Antic Hay', 7, '3597 Mayer Ferry', 206.9)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('014483802743049', 'East of Eden', 3, '91956 Douglas Vista', 479.14)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('011653621504649', 'The Soldiers Art', 3, '381 Okuneva Plains', 405.73)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('524486533091037', 'Whats Become of Waring', 10, '732 Hettinger Walks', 240.54)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('547897511298456', 'Consider the Lilies', 3, '397 Kamille Hill', 318.08)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('991712365102962', 'If Not Now, When?', 2, '16512 Brayan Camp', 403.27)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('994864168276373', 'Beneath the Bleeding', 10, '441 Marquardt Shoals', 234.81)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('014816846275462', 'The Curious Incident of the Dog in the Night-Time', 3, '594 Hans Gateway', 466.73)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('545339026700116', 'Jesting Pilate', 2, '594 Kris Crest', 145.61)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('443771414357476', 'Françoise Sagan', 9, '96098 Walter Mall', 24.67)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('352163038594131', 'Dulce et Decorum Est', 2, '429 Garry Shore', 258.78)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('998218874119425', 'Eyeless in Gaza', 2, '25665 Rice Key', 205.49)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('865079985182623', 'Fear and Trembling', 8, '25073 Reichel Shoals', 74.85)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('453416236509352', 'Fair Stood the Wind for France', 6, '9069 Yvonne Falls', 66.07)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('303208054547016', 'Mr Standfast', 4, '13038 Stiedemann Ridges', 174.1)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('916062703461643', 'Consider Phlebas', 4, '499 Wolff Springs', 337.64)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('109515167438115', 'In Death Ground', 8, '789 Macejkovic Drives', 488.19)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('519566010453060', 'Time To Murder And Create', 7, '3183 Virgil Lane', 446.19)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('519842439677278', 'Fame Is the Spur', 9, '5490 Nicolette Corners', 253.09)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('504343323486600', 'The Parliament of Man', 7, '91185 Myriam Stravenue', 227.77)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('100426894055512', 'The Mirror Crackd from Side to Side', 9, '73399 Murray Rest', 317.09)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('914223502107058', 'The Golden Apples of the Sun', 6, '2295 Rudolph Crest', 213.73)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('444847764321004', 'Quo Vadis', 5, '17260 Jacobi Springs', 15.81)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('918429155318506', 'A Time to Kill', 5, '293 Eunice Mills', 226.94)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('990192861112958', 'Cabbages and Kings', 3, '84759 Jerrell Manors', 160.81)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('446018106383619', 'Nectar in a Sieve', 3, '1850 Bernhard Squares', 71.9)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('507630808466819', 'Vanity Fair', 2, '7143 Daugherty Corners', 329.11)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('869261607005046', 'An Instant In The Wind', 10, '6472 Bria Landing', 95.49)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('518093466589785', 'No Country for Old Men', 7, '88751 Deckow Road', 343.1)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('993332885166104', 'If Not Now, When?', 1, '1859 Durgan Tunnel', 252.87)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('498918147783015', 'The Last Temptation', 4, '64913 Bartoletti Pass', 258.08)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('982191271990193', 'Ego Dominus Tuus', 4, '78729 White Club', 366.99)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('451660867250461', 'Vanity Fair', 6, '53130 Mossie Summit', 200.78)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('984175513357244', 'The Cricket on the Hearth', 4, '38708 Langworth Stream', 181.33)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('496128491382669', 'Jacob Have I Loved', 4, '269 Kilback Junction', 183.49)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('499010657624406', 'Bury My Heart at Wounded Knee', 9, '80765 Gerardo Light', 269.81)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('457851626008951', 'For Whom the Bell Tolls', 10, '88936 Tanner Locks', 371.33)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('986846213956570', 'Frequent Hearses', 2, '58515 Cruz Mission', 381.76)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('991093607277999', 'That Hideous Strength', 9, '90437 Abernathy Viaduct', 20.84)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('307860358909601', 'An Acceptable Time', 5, '34159 Zieme Forges', 135.35)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('981890865301987', 'The Man Within', 9, '6457 Alf Rest', 120.64)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('510640670424510', 'Specimen Days', 3, '234 Devin Brook', 93.05)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('997425069142960', 'The Way Through the Woods', 8, '4548 Eichmann Passage', 107.08)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('447039690912968', 'Nectar in a Sieve', 2, '291 Yost Walk', 281.48)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('502362600129917', 'All Passion Spent', 2, '21387 Chelsey Roads', 118.01)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('917612193007857', 'The Heart Is Deceitful Above All Things', 5, '7099 Botsford Field', 123.08)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('523903545808274', 'Wildfire at Midnight', 8, '5649 Runte Mill', 84.8)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('537728632141279', 'The Grapes of Wrath', 10, '3013 Montana Valley', 388.71)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('351589116840627', 'No Country for Old Men', 6, '68292 Rene Courts', 287.54)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('526805742735282', 'A Time of Gifts', 2, '9288 Jude Key', 275.78)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('916141963477679', 'The Wings of the Dove', 8, '84956 Bogan Ports', 367.12)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('010134806385071', 'The Heart Is a Lonely Hunter', 10, '147 Rhianna Square', 157.77)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('918383362483936', 'His Dark Materials', 5, '3540 Rodriguez Lodge', 186.47)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('912066265194017', 'A Handful of Dust', 2, '84602 MacGyver Mills', 231.01)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('525632472187414', 'Look to Windward', 8, '73914 Cristal Islands', 483.25)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('014847949292674', 'Sleep the Brave', 4, '32866 Walker Ramp', 292.11)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('521642261909757', 'This Side of Paradise', 2, '46935 Lesch Crossing', 267.93)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('548314661186480', 'Bloods a Rover', 10, '497 Stokes Mount', 133.1)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('537840560342207', 'A Confederacy of Dunces', 2, '2681 Lynch Forks', 351.23)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('330250199991557', 'Number the Stars', 4, '217 Dooley Unions', 203.31)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('917928271967329', 'Postern of Fate', 5, '1030 Considine Heights', 19.4)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('012470853914233', 'In Death Ground', 2, '9834 Edward Park', 91.33)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('309091091231917', 'Have His Carcase', 7, '22379 Karley Dam', 437.59)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('101128121634999', 'Mother Night', 6, '69964 Ebba Valley', 58.26)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('538846160845621', 'A Scanner Darkly', 6, '49371 Stokes Ridge', 160.38)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('547766389533015', 'Whats Become of Waring', 7, '8381 Darrick Creek', 156.97)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('862845008826948', 'Have His Carcase', 5, '8188 Runolfsson Summit', 256.57)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('867142279069316', 'The Needles Eye', 9, '22449 Olen Knolls', 336.09)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('108192989138140', 'A Many-Splendoured Thing', 6, '76113 Lance Flat', 53.0)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('333252881485580', 'A Catskill Eagle', 5, '10655 Alvah Drives', 288.85)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('015497695422700', 'In Dubious Battle', 1, '4367 Torp Mission', 436.86)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('011151847256540', 'Gone with the Wind', 2, '45215 Xzavier Falls', 330.0)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('867852580386585', 'Mr Standfast', 2, '61428 Okuneva Cove', 376.85)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('304033532578766', 'If I Forget Thee Jerusalem', 5, '708 Christiansen Estates', 196.77)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('861892212867087', 'Many Waters', 2, '1097 Hammes Cape', 318.16)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('108409967680509', 'A Darkling Plain', 8, '782 Durgan Rapids', 156.2)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('458006613841984', 'The Glory and the Dream', 2, '84164 Gleason Branch', 300.34)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('549572441434251', 'The Daffodil Sky', 2, '783 Bashirian Fork', 328.98)");
        stmt.execute("insert into TEST_LOOKUP_DB (name, value, period, address, price) VALUES ('is-a-null', NULL, 2, NULL, 328.98)");

    }

    /**
     * Simple implementation only from ExecuteSQL processor testing.
     */
    static class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcpService";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.hsqldb.jdbcDriver");
                return DriverManager.getConnection("jdbc:hsqldb:mem:test;sql.syntax_pgs=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}

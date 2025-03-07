import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
import canonada.catalog as catalog


class TestCSVDatahandlers(unittest.TestCase):
    """
    Test built in CSV datahandlers
    """

    def setUp(self):
        # Save the current directory
        self.original_directory = os.getcwd()
        # Change directory to the test directory
        os.chdir("tests")

    def tearDown(self):
        # Change back to the original directory
        os.chdir(self.original_directory)

    def test_csv_rows_no_keys(self):
        """
        Test the csv_rows datahandler without specifying keys
        """
        
        csv_rows_dh = catalog.available_datahandlers["canonada.csv_rows"](name="test_csv_rows", keys=[], kwargs={"path": "data/Flights1m.csv"})

        # Test length
        self.assertEqual(len(csv_rows_dh), 1000000, "Length of csv_rows datahandler is not correct")

        # Test iteration
        # Example data:
        # FL_DATE,DEP_DELAY,ARR_DELAY,AIR_TIME,DISTANCE,DEP_TIME,ARR_TIME
        # 2006-01-01,5,19,350,2475,9.083333,12.483334
        # 2006-01-02,167,216,343,2475,11.783334,15.766666
        # 2006-01-03,-7,-2,344,2475,8.883333,12.133333
        for i, row in csv_rows_dh:
            if i == 0:
                self.assertEqual(row["FL_DATE"], "2006-01-01", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "5", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "19", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "350", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "9.083333", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "12.483334", "ARR_TIME is not correct")
            if i == 1:
                self.assertEqual(row["FL_DATE"], "2006-01-02", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "167", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "216", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "343", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "11.783334", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "15.766666", "ARR_TIME is not correct")
            if i == 2:
                self.assertEqual(row["FL_DATE"], "2006-01-03", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "-7", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "-2", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "344", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "8.883333", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "12.133333", "ARR_TIME is not correct")
                break
        
        # Test getitem
        row = csv_rows_dh[0]
        self.assertEqual(row["FL_DATE"], "2006-01-01", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "5", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "19", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "350", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "9.083333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.483334", "ARR_TIME is not correct")

        row = csv_rows_dh[1]
        self.assertEqual(row["FL_DATE"], "2006-01-02", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "167", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "216", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "343", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "11.783334", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "15.766666", "ARR_TIME is not correct")

        row = csv_rows_dh[2]
        self.assertEqual(row["FL_DATE"], "2006-01-03", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "-7", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "-2", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "344", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "8.883333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.133333", "ARR_TIME is not correct")
        
        # Test save
        csv_rows_dh_save = catalog.available_datahandlers["canonada.csv_rows"](name="test_csv_rows_save", keys=[], kwargs={"path": "data/Flights1m_test.csv", "headers": ["FL_DATE", "DEP_DELAY", "ARR_DELAY", "AIR_TIME", "DISTANCE", "DEP_TIME", "ARR_TIME"]})
        for i, row in csv_rows_dh:
            csv_rows_dh_save.save(row)
        
        # Rebuild the index (this is only necessary for testing purposes)
        csv_rows_dh_save.__init__(name="test_csv_rows_save", keys=[], kwargs={"path": "data/Flights1m_test.csv"})
        self.assertEqual(len(csv_rows_dh_save), 1_000_000, "Length of csv_rows datahandler is not correct")

        for i, row in csv_rows_dh_save:
            self.assertEqual(row["FL_DATE"], csv_rows_dh[i]["FL_DATE"], "Saved FL_DATE is not correct")
            self.assertEqual(row["DEP_DELAY"], csv_rows_dh[i]["DEP_DELAY"], "Saved DEP_DELAY is not correct")
            self.assertEqual(row["ARR_DELAY"], csv_rows_dh[i]["ARR_DELAY"], "Saved ARR_DELAY is not correct")
            self.assertEqual(row["AIR_TIME"], csv_rows_dh[i]["AIR_TIME"], "Saved AIR_TIME is not correct")
            self.assertEqual(row["DISTANCE"], csv_rows_dh[i]["DISTANCE"], "Saved DISTANCE is not correct")
            self.assertEqual(row["DEP_TIME"], csv_rows_dh[i]["DEP_TIME"], "Saved DEP_TIME is not correct")
            self.assertEqual(row["ARR_TIME"], csv_rows_dh[i]["ARR_TIME"], "Saved ARR_TIME is not correct")

        # Clean up
        os.remove("data/Flights1m_test.csv")

    def test_csv_rows_keys(self):
        """
        Test the csv_rows datahandler specifying keys
        """

        csv_rows_dh = catalog.available_datahandlers["canonada.csv_rows"](name="test_csv_rows_keys", keys=["FL_DATE"], kwargs={"path": "data/Flights1m.csv"})

        # Test length (should be less than 1000000 because the key is not unique)
        self.assertNotEqual(len(csv_rows_dh), 1000000, "Length of csv_rows datahandler is not correct")
        self.assertEqual(len(csv_rows_dh), 59, "Length of csv_rows datahandler is not correct")

        # Test iteration (should return the full 1M rows)
        # Example data:
        # FL_DATE,DEP_DELAY,ARR_DELAY,AIR_TIME,DISTANCE,DEP_TIME,ARR_TIME
        # 2006-01-01,5,19,350,2475,9.083333,12.483334
        # 2006-01-02,167,216,343,2475,11.783334,15.766666
        # 2006-01-03,-7,-2,344,2475,8.883333,12.133333
        for i, row in csv_rows_dh:
            if i == 0:
                self.assertEqual(row["FL_DATE"], "2006-01-01", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "5", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "19", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "350", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "9.083333", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "12.483334", "ARR_TIME is not correct")
            if i == 1:
                self.assertEqual(row["FL_DATE"], "2006-01-02", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "167", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "216", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "343", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "11.783334", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "15.766666", "ARR_TIME is not correct")
            if i == 2:
                self.assertEqual(row["FL_DATE"], "2006-01-03", "FL_DATE is not correct")
                self.assertEqual(row["DEP_DELAY"], "-7", "DEP_DELAY is not correct")
                self.assertEqual(row["ARR_DELAY"], "-2", "ARR_DELAY is not correct")
                self.assertEqual(row["AIR_TIME"], "344", "AIR_TIME is not correct")
                self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
                self.assertEqual(row["DEP_TIME"], "8.883333", "DEP_TIME is not correct")
                self.assertEqual(row["ARR_TIME"], "12.133333", "ARR_TIME is not correct")
                break
        
        # Test getitem (should only return the first row for each key in this case max 59 rows)
        row = csv_rows_dh[("2006-01-01",)]
        self.assertEqual(row["FL_DATE"], "2006-01-01", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "5", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "19", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "350", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "9.083333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.483334", "ARR_TIME is not correct")

        row = csv_rows_dh[("2006-01-02",)]
        self.assertEqual(row["FL_DATE"], "2006-01-02", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "167", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "216", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "343", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "11.783334", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "15.766666", "ARR_TIME is not correct")

        row = csv_rows_dh[("2006-01-31",)]
        self.assertEqual(row["FL_DATE"], "2006-01-31", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "-4", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "-8", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "344", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "8.933333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.033334", "ARR_TIME is not correct")

        # Reload the datahandler with two keys that make more records unique
        csv_rows_dh = catalog.available_datahandlers["canonada.csv_rows"](name="test_csv_rows_keys", keys=["FL_DATE", "DEP_TIME", "DISTANCE"], kwargs={"path": "data/Flights1m.csv"})

        # Test length (should be 983283 because the keys are not unique)
        self.assertEqual(len(csv_rows_dh), 983283, "Length of csv_rows datahandler is not correct")

        # Test getitem (should return the correct row)
        row = csv_rows_dh[("2006-01-01", "9.083333", "2475")]
        self.assertEqual(row["FL_DATE"], "2006-01-01", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "5", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "19", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "350", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "9.083333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.483334", "ARR_TIME is not correct")

        row = csv_rows_dh[("2006-01-02", "11.783334", "2475")]
        self.assertEqual(row["FL_DATE"], "2006-01-02", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "167", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "216", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "343", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "11.783334", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "15.766666", "ARR_TIME is not correct")

        row = csv_rows_dh[("2006-01-03", "8.883333", "2475")]
        self.assertEqual(row["FL_DATE"], "2006-01-03", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "-7", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "-2", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "344", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "2475", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "8.883333", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "12.133333", "ARR_TIME is not correct")

        # 2006-01-05,12,18,103,516,17.45,19.483334
        row = csv_rows_dh[("2006-01-05", "17.45", "516")]
        self.assertEqual(row["FL_DATE"], "2006-01-05", "FL_DATE is not correct")
        self.assertEqual(row["DEP_DELAY"], "12", "DEP_DELAY is not correct")
        self.assertEqual(row["ARR_DELAY"], "18", "ARR_DELAY is not correct")
        self.assertEqual(row["AIR_TIME"], "103", "AIR_TIME is not correct")
        self.assertEqual(row["DISTANCE"], "516", "DISTANCE is not correct")
        self.assertEqual(row["DEP_TIME"], "17.45", "DEP_TIME is not correct")
        self.assertEqual(row["ARR_TIME"], "19.483334", "ARR_TIME is not correct")


if __name__ == "__main__":
    unittest.main()
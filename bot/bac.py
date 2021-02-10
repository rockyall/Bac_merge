import os
import pyodbc
import datetime
import numpy
from dbServer import db_server


class bac_credomatic:
    def __init__(self, sql, path):
        self.Sql = sql
        self.path = path
        self.transaction_data = []
        self.transaction_profile = []

    def merge_transaction(self, id):
        try:
            # Prepare the data
            tablename = "transactionsdetails"
            file = open(self.path, mode='r')
            rows = file.readlines()

            # Iterate for the each transaction row
            fields = ["Date", "Reference", "Code",
                      "Description", "Debit", "Credit", "Balance", "TransactionProfileId"]
            # Now we have to filter the array by the date
            query = f"select top 1 Date from {tablename} order by [Date] DESC;"
            query2 = f"SELECT Date from {tablename} order by Date DESC Limit 1;"
            last_transaction = self.Sql.get_data_query(query2)

            index = 0
            for row in rows[5:(len(rows) - 13)]:
                columns = row.split(',')
                dateArray = columns[0].split("/")

                # Clean the data from white spaces
                Reference = columns[1].strip()
                Code = columns[2].strip()
                Description = columns[3].strip()
                Debit = columns[4].strip()
                Credit = columns[5].strip()
                Balance = columns[6].strip()

                date = datetime.datetime(int(dateArray[2]), int(
                    dateArray[1]), int(dateArray[0]))
                transaction_array = [date.strftime(
                    "%Y-%m-%d"), Reference, Code, Description, float(Debit), float(Credit), float(Balance), id]
                if(type(last_transaction) == int):
                    self.transaction_data.append(transaction_array)
                    continue

                if(date > last_transaction[0]):
                    self.transaction_data.append(transaction_array)

                index += 1
            if(len(self.transaction_data) > 0):
                self.Sql.insert_rows(
                    tablename, fields, self.transaction_data)

            file.close()
        except Exception as ex:
            file.close()
            print(ex)

    def merge_transaction_profile(self):
        try:
            tablename = "transactionsprofiles"
            rows = []
            if os.path.exists(self.path):
                file = open(self.path, mode='r')
                rows = file.readlines()

            # Profile
            columns = rows[1].split(",")
            profile = {
                "Name": columns[1].strip(),
                "Product": columns[2].strip(),
                "Currency": columns[3].strip(),
                "InitialRate": columns[4].strip(),
                "BalanceInBooks": columns[5].strip(),
                "HeldDeferred": columns[6].strip(),
                "AvialableBalance": columns[7].strip(),
                "LastUpdated": datetime.datetime(int(columns[8].split("/")[2]), int(columns[8].split("/")[1]), int(columns[8].split("/")[0]))
            }

            profileID = self.Sql.get_profile_id_by_product(
                tablename, profile["Product"])
            if(profileID == 0):
                fields = ["Name", "Product", "Currency", "InitialRate",
                          "BalanceInBooks", "HeldDeferred", "AvialableBalance", "LastUpdated"]
                self.transaction_profile = [profile["Name"], profile["Product"], profile["Currency"],
                                            float(profile["InitialRate"]), float(
                                                profile["BalanceInBooks"]), float(profile["HeldDeferred"]),
                                            float(profile["AvialableBalance"]), profile["LastUpdated"].strftime("%Y-%m-%d")]

                self.Sql.insert_rows(tablename, fields, [
                                     self.transaction_profile])
                profileID = self.Sql.get_profile_id_by_product(tablename,
                                                               profile["Product"])
                return profileID
            else:
                return profileID

        except Exception as ex:
            print(ex)


# Execute this part for execute the code
# if __name__ == "__main__":
#     ServerName = "localhost"
#     DB = "db_finance"
#     user = "root"
#     password = "root"
#     csvPath = "Transacciones del mes.csv"

#     DBServer = db_server()
#     DBServer.get_mysql_connection(ServerName, DB, user, password)
#     Bac = bac_credomatic(DBServer, csvPath)
#     profile_id = Bac.merge_transaction_profile()
#     Bac.merge_transaction(profile_id)

#     DBServer.close_connection()

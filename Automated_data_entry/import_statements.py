import pandas as pd
import set_paths as path
import os
import csv
import numpy as np
import datetime


class Boc_statement:

	def __init__(self, file_name, file_path):

		def get_csv_data(path, file_name):

			with open(path + "\\" + file_name) as csvfile:
				csv_data = []
				csv_reader = csv.reader(csvfile)
				for line in csv_reader:
					csv_data.append(line)
			return csv_data

		def get_full_transactions(self):

			transactions = []
			for line in self.csv_data[self.state_data_divider:]:
				transactions.append(line)
			return transactions

		def get_amount_col(self):

			debits = []
			credits = []
			for line in self.transactions[1:]:
				if str.strip(line[5]) == "":
					debits.append(0)
				else:
					debits.append(float(str.strip(line[5])))
				if str.strip(line[6]) == "":
					credits.append(0)
				else:
					credits.append(float(str.strip(line[6])))
			np_debits = np.array(debits)
			np_credits = np.array(credits)
			np_amounts = np_credits-np_debits

			return np_amounts

		def get_date_cols(self):

			dates = []
			for line in self.transactions[1:]:

				try:
					dates.append(datetime.datetime.strptime(line[1], "%d/%m/%Y"))
				except:
					dates.append(datetime.datetime.strptime("01/01/1970", "%d/%m/%Y"))
			np_dates = np.array(dates)
			return dates

		def get_value_date_cols(self):

			value_dates = []
			for line in self.transactions[1:]:
				try:
					value_dates.append(datetime.datetime.strptime(line[2], "%d/%m/%Y"))
				except:
					value_dates.append("error_data")
			np_value_dates = np.array(value_dates)
			return np_value_dates	

		def get_bank_refs(self):

			bank_refs = []
			for line in self.transactions[1:]:
				bank_refs.append(line[0])
			np_bank_refs = np.array(bank_refs)

			return np_bank_refs

		def get_origin_branch(self):

			origin_branches = []

			for line in self.transactions[1:]:
				origin_branches.append(line[-1])
			np_origin_branches = np.array(origin_branches)
			return origin_branches

		def get_description(self):

			descriptions = []

			for line in self.transactions[1:]:
				descriptions.append(line[3])
			np_descritions = np.array(descriptions)

			return np_descritions

		def get_our_ref(self):

			Account_names = {"COMPANY 1 LTD": "CY01-PUB-BCYP", "COMPANY 2": "CY02-BOX-BCYP" }

			return Account_names[str.strip(self.account_name)]

		def get_our_acc_sid(self):

			if int(float(str.strip(self.account_number))) == 00000000000:
				acc_sid = "CARD"
			else:
				acc_sid = "MAIN"
			return acc_sid

		def build_data_dict(self):

			data_dict = {}

			data_dict["Acc MID"] = [self.our_acc_ref]*(len(self.transactions)-1)
			data_dict["Acc SID"] = [self.our_acc_sid]*(len(self.transactions)-1)
			data_dict["Amount"] = self.amounts
			data_dict["CUR"] = [self.currency]*(len(self.transactions)-1)
			data_dict["Date"] = self.value_dates
			data_dict["Details"] = self.description
	

			return data_dict

		def build_dataframe(self):

			df = pd.DataFrame(self.data_dict)
			
			df["CP MID"] = ""

			df["CC"] = ""

			df["CP SID"] = ""

			df["GS Article"] = ""

			return df

		def get_balance(self):

			ind_balances = []

			for line in self.transactions[1:]:
				ind_balances.append(line[7])
			np_ind_balances = np.array(ind_balances)

			return np_ind_balances

		def get_state_data_divider(self):
			divider = 0
			for i, line in enumerate(self.csv_data):
				if str.strip(line[0]) == "Bank Reference Number":
					divider = i
					break
			return divider

		def get_state_data_dict(self):

			state_data_dict = {}
			for line in self.csv_data[:self.state_data_divider]:
				state_data_dict[str.strip(line[0])] = str.strip(line[1])

			return state_data_dict

		self.file_name = file_name
		self.csv_data = get_csv_data(file_path, file_name)
		self.state_data_divider = get_state_data_divider(self)
		self.state_data_dict = get_state_data_dict(self)
		self.period = self.state_data_dict["Period:"]
		self.account_number = self.state_data_dict["Account Number:"]
		self.account_name = self.state_data_dict["Account Name:"]
		self.account_type = self.state_data_dict["Account Type:"]
		self.currency = self.state_data_dict["Account Currency:"]
		self.transactions = get_full_transactions(self)
		self.amounts = get_amount_col(self)
		self.dates = get_date_cols(self)
		self.value_dates = get_value_date_cols(self)
		self.bank_refs = get_bank_refs(self)
		self.origin_branches = get_origin_branch(self)
		self.description = get_description(self)
		self.balance = get_balance(self)
		self.our_acc_ref = get_our_ref(self)
		self.our_acc_sid = get_our_acc_sid(self)
		self.data_dict = build_data_dict(self)
		self.df_trans = build_dataframe(self)

class Processed_data:

	def __init__(self, file_path):

		file_names = os.listdir(file_path)


		Boc_statement_objects = [Boc_statement(file_name, file_path) for file_name in file_names]

		def stack_data_frames(statement_obj):

			df_list = []
			for obj in statement_obj:
				df_list.append(obj.df_trans)

			full_df = pd.concat(df_list).reset_index(drop = True)

			# full_df = full_df.drop_duplicates()

			full_df = full_df[full_df.Date != datetime.datetime.strptime("01/01/1970", "%d/%m/%Y")]

			# full_df.Date =  pd.to_datetime(full_df.Date, format = "%d/%m/%Y" )


			return full_df

		self.processed_dataframe = stack_data_frames(Boc_statement_objects)

def writer(destination, file_name, df):
	os.chdir(destination)
	df.to_csv(file_name)
